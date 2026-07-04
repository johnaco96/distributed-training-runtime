"""TLS startup regression test.

Guards against the rustls "could not determine CryptoProvider" panic that broke
ALL TLS (`--tls-cert`) use: the coordinator must actually serve gRPC over TLS
without crashing. Gated by DTRUNTIME_RUN_LOCAL_INTEGRATION=1 and requires the
prebuilt release coordinator + openssl.
"""

from __future__ import annotations

import os
import shutil
import subprocess

import pytest

import ops_common as ops

pytestmark = pytest.mark.skipif(
    os.environ.get("DTRUNTIME_RUN_LOCAL_INTEGRATION") != "1",
    reason="set DTRUNTIME_RUN_LOCAL_INTEGRATION=1 to run the TLS integration test",
)


def _make_self_signed_cert(tmp_path):
    """Generate a self-signed localhost cert/key with openssl."""
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl not available")
    cert = tmp_path / "tls.crt"
    key = tmp_path / "tls.key"
    subprocess.run(
        [
            openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(key), "-out", str(cert),
            "-days", "1", "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True, capture_output=True,
    )
    return cert, key


def test_coordinator_serves_over_tls_without_panicking(tmp_path):
    pytest.importorskip("grpc")
    from dtruntime.client import RuntimeClient

    binary = ops.require_binary("runtime-coordinator")
    cert, key = _make_self_signed_cert(tmp_path)
    address = f"127.0.0.1:{ops.free_tcp_port()}"

    proc = ops.start_coordinator(
        binary, address,
        ["--metadata-db", str(tmp_path / "meta.db"),
         "--storage-root", str(tmp_path / "storage"),
         "--tls-cert", str(cert), "--tls-key", str(key)],
    )
    try:
        # If the crypto-provider regression returns, the process panics/exits
        # here instead of becoming reachable.
        deadline_ok = False
        import grpc, time
        creds = grpc.ssl_channel_credentials(root_certificates=cert.read_bytes())
        end = time.monotonic() + 30
        last = None
        while time.monotonic() < end:
            if proc.poll() is not None:
                out = proc.stdout.read() if proc.stdout else ""
                raise AssertionError(f"coordinator exited early over TLS:\n{out}")
            ch = grpc.secure_channel(address, creds)
            try:
                grpc.channel_ready_future(ch).result(timeout=2)
                deadline_ok = True
                break
            except Exception as e:  # noqa: BLE001
                last = e
            finally:
                ch.close()
        assert deadline_ok, f"coordinator not reachable over TLS: {last}"

        # A real RPC over TLS succeeds.
        client = RuntimeClient(address, tls_ca_cert=str(cert))
        client.list_runs(timeout=8)
        client.close()
    finally:
        ops.stop_process(proc)
