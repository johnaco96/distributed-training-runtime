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


def test_generate_dev_certs_script_includes_kubernetes_service_sans():
    content = Path("scripts/generate_dev_certs.ps1").read_text()
    assert "DNS.1=localhost" in content
    assert "IP.1=127.0.0.1" in content
    assert "DNS.2=runtime-coordinator" in content
    assert "DNS.3=runtime-coordinator.dtruntime" in content
    assert "DNS.4=runtime-coordinator.dtruntime.svc" in content
    assert "DNS.5=runtime-coordinator.dtruntime.svc.cluster.local" in content
