# End-to-end validation of the Kubernetes operator inside kind:
#   TrainingRun -> operator -> managed coordinator + Indexed worker Job ->
#   2-node gloo DDP training -> status Succeeded.
#
# Prereqs: docker, kind, kubectl, cargo. Run from the repo root:
#   pwsh -File scripts/kind_operator_e2e.ps1
#
# The operator runs locally (cargo run) against the kind API server, so no
# operator image is needed; the combined coordinator+trainer image is built
# from ./Dockerfile and side-loaded into kind.

$ErrorActionPreference = "Stop"
$cluster = "dtr-e2e"
$image = "dtruntime:e2e"
$ns = "training"

function Step($msg) { Write-Host "==> $msg" -ForegroundColor Cyan }

Step "Ensure kind cluster '$cluster'"
$clusters = kind get clusters 2>$null
if ($clusters -notcontains $cluster) {
    kind create cluster --name $cluster --wait 120s
}

Step "Build and load $image"
docker build -t $image .
if ($LASTEXITCODE -ne 0) { throw "docker build failed" }
kind load docker-image $image --name $cluster
if ($LASTEXITCODE -ne 0) { throw "kind load failed" }

Step "Apply CRD and namespace"
kubectl --context "kind-$cluster" apply -f k8s/operator/crd-trainingrun.yaml
kubectl --context "kind-$cluster" create namespace $ns --dry-run=client -o yaml | kubectl --context "kind-$cluster" apply -f -

Step "Start operator locally against kind"
$operator = Start-Process -PassThru -NoNewWindow cargo -ArgumentList @(
    "run", "-q", "-p", "runtime-operator", "--bin", "runtime-operator"
) -Environment @{ KUBECONFIG = "$env:USERPROFILE\.kube\config"; RUST_LOG = "info" }

try {
    Step "Submit TrainingRun (2 nodes x 1 proc, gloo, managed coordinator)"
    @"
apiVersion: dtruntime.io/v1alpha1
kind: TrainingRun
metadata:
  name: e2e-ddp
  namespace: $ns
spec:
  image: $image
  script: ["python/examples/pytorch_ddp_train.py", "--steps", "10"]
  nodes: 2
  nprocPerNode: 1
  backend: gloo
  coordinator:
    manage:
      image: $image
"@ | kubectl --context "kind-$cluster" apply -f -

    Step "Wait for phase Succeeded (up to 10 minutes)"
    $deadline = (Get-Date).AddMinutes(10)
    while ($true) {
        $phase = kubectl --context "kind-$cluster" -n $ns get trainingrun e2e-ddp -o jsonpath='{.status.phase}' 2>$null
        $msg = kubectl --context "kind-$cluster" -n $ns get trainingrun e2e-ddp -o jsonpath='{.status.message}' 2>$null
        Write-Host "   phase=$phase  $msg"
        if ($phase -eq "Succeeded") { break }
        if ($phase -eq "Failed") {
            kubectl --context "kind-$cluster" -n $ns get pods -o wide
            kubectl --context "kind-$cluster" -n $ns logs -l "app.kubernetes.io/component=worker" --tail 50
            throw "TrainingRun failed: $msg"
        }
        if ((Get-Date) -gt $deadline) { throw "timed out waiting for Succeeded (last: $phase)" }
        Start-Sleep -Seconds 10
    }

    Step "PASS - worker logs:"
    kubectl --context "kind-$cluster" -n $ns logs -l "app.kubernetes.io/component=worker" --tail 20
}
finally {
    Step "Cleanup"
    kubectl --context "kind-$cluster" -n $ns delete trainingrun e2e-ddp --ignore-not-found
    Stop-Process -Id $operator.Id -Force -ErrorAction SilentlyContinue
}
Write-Host "e2e complete. Delete the cluster with: kind delete cluster --name $cluster"
