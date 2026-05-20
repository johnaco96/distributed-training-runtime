from pathlib import Path


def test_step4_script_contains_required_contracts() -> None:
    content = Path("scripts/run_k8s_secure_validation.ps1").read_text(encoding="utf-8")

    required_snippets = [
        "ImageTag",
        "validation-",
        "set image deployment/runtime-coordinator",
        "pytorch-coordinator-job.template.yaml",
        "summary.json",
        "summary.csv",
        "metrics.prom",
        "/metrics",
        "tls_enabled",
        "mtls_enabled",
        "hostname_verification_expected",
        "metadata_auth_enabled",
        "admin_worker_identity_separation",
    ]

    for snippet in required_snippets:
        assert snippet in content
