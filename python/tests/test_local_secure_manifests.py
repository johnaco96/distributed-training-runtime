from pathlib import Path


def test_secure_coordinator_manifest_secret_mounts_and_tls_flags():
    content = Path("k8s/local-secure/coordinator-deployment.yaml").read_text()
    assert "secretName: dtruntime-tls" in content
    assert "secretName: dtruntime-client-ca" in content
    assert "--tls-cert" in content
    assert "/etc/dtruntime/tls/server/tls.crt" in content
    assert "--tls-key" in content
    assert "/etc/dtruntime/tls/server/tls.key" in content
    assert "--tls-client-ca" in content
    assert "/etc/dtruntime/tls/client-ca/ca.crt" in content


def test_secure_job_manifest_client_tls_env_and_secrets():
    content = Path("k8s/local-secure/pytorch-coordinator-job.yaml").read_text()
    assert "runtime-coordinator.dtruntime.svc.cluster.local:50051" in content
    assert "DTRUNTIME_TLS_CA_CERT" in content
    assert "DTRUNTIME_TLS_CLIENT_CERT" in content
    assert "DTRUNTIME_TLS_CLIENT_KEY" in content
    assert "secretName: dtruntime-client-ca" in content
    assert "secretName: dtruntime-client-tls" in content
