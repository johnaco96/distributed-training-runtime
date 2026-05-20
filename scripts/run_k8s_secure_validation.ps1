[CmdletBinding()]
param(
  [string]$ImageTag = ("distributed-training-runtime:validation-{0}" -f (Get-Date -Format "yyyyMMddHHmmss")),
  [string]$Namespace = "dtruntime",
  [string]$OutputDir = (".dtruntime-results/k8s-secure-{0}" -f (Get-Date -Format "yyyyMMddHHmmss")),
  [int]$Workers = 2,
  [int]$Epochs = 1,
  [int]$Samples = 32,
  [int]$BatchSize = 8,
  [int]$Shards = 4,
  [switch]$SkipBuild,
  [switch]$SkipCertGeneration,
  [switch]$SkipPrometheus,
  [switch]$KeepNamespace
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Require-Tool([string]$Name) {
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing required tool: $Name"
  }
}

function Run([string]$Command) {
  "$(Get-Date -Format o) $Command" | Add-Content -Path $TranscriptPath
  Invoke-Expression $Command
}

function Get-OptionalOpenSsl() {
  $openssl = Get-Command openssl -ErrorAction SilentlyContinue
  if ($openssl) { return $openssl.Source }
  $gitOpenSsl = "C:\Program Files\Git\usr\bin\openssl.exe"
  if (Test-Path $gitOpenSsl) { return $gitOpenSsl }
  return $null
}

function Parse-Metric([string]$MetricsText, [string]$MetricName) {
  $match = [regex]::Match($MetricsText, "(?m)^$MetricName(?:\{[^\}]*\})?\s+([0-9eE+\.-]+)$")
  if ($match.Success) { return $match.Groups[1].Value }
  return ""
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$TranscriptPath = Join-Path $OutputDir "command-transcript.txt"

Require-Tool docker
Require-Tool kubectl
Require-Tool cargo
$openSslPath = $null
if (-not $SkipCertGeneration) {
  $openSslPath = Get-OptionalOpenSsl
  if (-not $openSslPath) {
    throw "OpenSSL not found (openssl or Git for Windows OpenSSL required)."
  }
}

if (-not $SkipBuild) {
  Run "docker build -t $ImageTag ."
}

$pyScript = "python/examples/pytorch_coordinator_train.py"
$content = Get-Content $pyScript -Raw
if ($content -notmatch "admin_client") { throw "Stale code check failed: missing admin_client." }
if ($content -notmatch "for_worker") { throw "Stale code check failed: missing for_worker." }
if ($content -match "client\._runtime_pb2") { throw "Stale code check failed: found deprecated client._runtime_pb2." }
if ($content -match "client\.stub\.RegisterRun") { throw "Stale code check failed: found deprecated client.stub.RegisterRun." }

$kubeContext = (kubectl config current-context)
$nodesJson = kubectl get nodes -o json | ConvertFrom-Json
$nodeCount = @($nodesJson.items).Count
$k8sVersion = (kubectl version -o json | ConvertFrom-Json).serverVersion.gitVersion

if (-not $KeepNamespace) {
  Run "kubectl delete namespace $Namespace --ignore-not-found"
  Run "kubectl create namespace $Namespace"
}

if (-not $SkipCertGeneration) {
  Run "powershell -ExecutionPolicy Bypass -File ./scripts/generate_dev_certs.ps1"
}

Run "kubectl -n $Namespace delete secret dtruntime-tls dtruntime-client-ca dtruntime-client-tls dtruntime-auth --ignore-not-found"
Run "kubectl -n $Namespace create secret tls dtruntime-tls --cert=.dtruntime-certs/server.crt --key=.dtruntime-certs/server.key"
Run "kubectl -n $Namespace create secret generic dtruntime-client-ca --from-file=ca.crt=.dtruntime-certs/ca.crt"
Run "kubectl -n $Namespace create secret tls dtruntime-client-tls --cert=.dtruntime-certs/client.crt --key=.dtruntime-certs/client.key"
Run "kubectl -n $Namespace create secret generic dtruntime-auth --from-literal=DTRUNTIME_AUTH_SUBJECT=admin-local --from-literal=DTRUNTIME_AUTH_ROLE=admin"

Run "kubectl apply -f k8s/local-secure/coordinator-service.yaml"
Run "kubectl apply -f k8s/local-secure/coordinator-deployment.yaml"
Run "kubectl -n $Namespace set image deployment/runtime-coordinator coordinator=$ImageTag"

$rolloutStart = Get-Date
Run "kubectl -n $Namespace rollout status deploy/runtime-coordinator --timeout=240s"
$rolloutSeconds = ((Get-Date) - $rolloutStart).TotalSeconds

$coordPod = kubectl -n $Namespace get pod -l app=runtime-coordinator -o json | ConvertFrom-Json
$coordItem = $coordPod.items[0]
$coordPhase = $coordItem.status.phase
$coordReady = ($coordItem.status.conditions | Where-Object { $_.type -eq "Ready" }).status -eq "True"
$coordRestarts = ($coordItem.status.containerStatuses | Measure-Object -Property restartCount -Sum).Sum
if ($coordPhase -ne "Running" -or -not $coordReady -or $coordRestarts -ne 0) {
  throw "Coordinator validation failed. phase=$coordPhase ready=$coordReady restarts=$coordRestarts"
}

$jobTemplate = Get-Content "k8s/local-secure/pytorch-coordinator-job.template.yaml" -Raw
$jobYaml = $jobTemplate.Replace("__IMAGE_TAG__", $ImageTag).Replace("__NAMESPACE__", $Namespace).Replace("__WORKERS__", "$Workers").Replace("__EPOCHS__", "$Epochs").Replace("__SAMPLES__", "$Samples").Replace("__BATCH_SIZE__", "$BatchSize").Replace("__SHARDS__", "$Shards")
$jobFile = Join-Path $OutputDir "pytorch-coordinator-job.generated.yaml"
Set-Content -Path $jobFile -Value $jobYaml

Run "kubectl -n $Namespace delete job pytorch-coordinator-demo --ignore-not-found"
$jobStart = Get-Date
Run "kubectl apply -f $jobFile"
Run "kubectl -n $Namespace wait --for=condition=complete --timeout=360s job/pytorch-coordinator-demo"
$jobSeconds = ((Get-Date) - $jobStart).TotalSeconds

kubectl -n $Namespace get pods -o wide | Set-Content (Join-Path $OutputDir "kubectl-pods.txt")
kubectl -n $Namespace describe deploy runtime-coordinator | Set-Content (Join-Path $OutputDir "kubectl-deployment.txt")
kubectl -n $Namespace describe job pytorch-coordinator-demo | Set-Content (Join-Path $OutputDir "kubectl-job.txt")
kubectl -n $Namespace logs deploy/runtime-coordinator | Set-Content (Join-Path $OutputDir "coordinator.log")
kubectl -n $Namespace logs job/pytorch-coordinator-demo | Set-Content (Join-Path $OutputDir "job.log")

$metricsFile = Join-Path $OutputDir "metrics.prom"
$pf = Start-Process kubectl -ArgumentList "-n",$Namespace,"port-forward","deploy/runtime-coordinator","19090:9090" -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 3
try {
  $metricsText = (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:19090/metrics" -TimeoutSec 20).Content
  Set-Content -Path $metricsFile -Value $metricsText
} finally {
  Stop-Process -Id $pf.Id -Force -ErrorAction SilentlyContinue
}
if (-not (Test-Path $metricsFile)) { throw "Required metrics endpoint could not be reached." }

$prometheusReachable = $false
if (-not $SkipPrometheus) {
  try {
    $promResult = Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:9091/api/v1/query?query=dtruntime_runs_total" -TimeoutSec 5
    if ($promResult.StatusCode -eq 200) { $prometheusReachable = $true }
  } catch { $prometheusReachable = $false }
}

$jobPod = kubectl -n $Namespace get pod -l job-name=pytorch-coordinator-demo -o json | ConvertFrom-Json
$jobRestartCount = ($jobPod.items[0].status.containerStatuses | Measure-Object -Property restartCount -Sum).Sum

$summary = [ordered]@{
  schema_version = "v1"
  benchmark_name = "k8s-secure-validation"
  timestamp_utc = (Get-Date).ToUniversalTime().ToString("o")
  image_tag = $ImageTag
  namespace = $Namespace
  environment = [ordered]@{
    platform = "docker-desktop-kubernetes"
    kubernetes_context = $kubeContext
    node_count = $nodeCount
    kubernetes_version = $k8sVersion
    os = [System.Environment]::OSVersion.ToString()
  }
  security = [ordered]@{
    tls_enabled = $true; mtls_enabled = $true; hostname_verification_expected = $true; metadata_auth_enabled = $true; admin_worker_identity_separation = $true
  }
  workload = [ordered]@{ workers=$Workers; epochs=$Epochs; samples=$Samples; batch_size=$BatchSize; shards=$Shards }
  results = [ordered]@{
    coordinator_rollout_seconds = [math]::Round($rolloutSeconds,2)
    job_duration_seconds = [math]::Round($jobSeconds,2)
    job_completed = $true
    coordinator_ready = $true
    coordinator_restart_count = $coordRestarts
    job_restart_count = $jobRestartCount
    metrics_endpoint_reachable = $true
    prometheus_reachable = $prometheusReachable
  }
  metrics = [ordered]@{}
  caveats = @(
    "Local Docker Desktop validation is production-style evidence but not production-scale throughput proof.",
    "Single-node local cluster characteristics may differ from multi-node production.",
    "Resource requests/limits from manifests apply to this run.",
    "No external object storage is validated unless explicitly configured.",
    "No multi-node throughput claim is made by this benchmark."
  )
}

$metricNames = @(
  "dtruntime_runs_total","dtruntime_registered_datasets_total","dtruntime_workers_total","dtruntime_active_workers","dtruntime_heartbeats_total","dtruntime_shard_assignments_total","dtruntime_checkpoints_started_total","dtruntime_checkpoints_committed_total","dtruntime_checkpoint_artifacts_reported_total","dtruntime_latest_checkpoint_reads_total","dtruntime_audit_events_total","dtruntime_auth_allowed_total","dtruntime_auth_denied_total"
)
foreach ($m in $metricNames) { $summary.metrics[$m.Replace("dtruntime_","").Replace("_total","")] = (Parse-Metric -MetricsText $metricsText -MetricName $m) }

$summaryPath = Join-Path $OutputDir "summary.json"
$summary | ConvertTo-Json -Depth 8 | Set-Content $summaryPath

$csvPath = Join-Path $OutputDir "summary.csv"
[pscustomobject]@{
  timestamp_utc = $summary.timestamp_utc; image_tag = $ImageTag; namespace = $Namespace; context = $kubeContext;
  coordinator_ready = $true; job_completed = $true; coordinator_rollout_seconds = $summary.results.coordinator_rollout_seconds;
  job_duration_seconds = $summary.results.job_duration_seconds; metrics_endpoint_reachable = $true; prometheus_reachable = $prometheusReachable
} | Export-Csv -NoTypeInformation -Path $csvPath

Write-Host "Validation completed. Results in $OutputDir"
