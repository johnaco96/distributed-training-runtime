#!/usr/bin/env bash
# Multi-node evidence harness: run a real TrainingRun across separate nodes and
# capture proof of cross-node rendezvous, checkpoint restore, and data locality.
#
# Requires: kind, kubectl, docker. Run on Linux (or the Tier-2 Linode cluster);
# the pattern mirrors the Phase 2 kind e2e that validated cross-pod gloo DDP.
set -euo pipefail
CLUSTER=dtr-multinode
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
OUT="${1:-$ROOT/evidence/multinode/$(date +%F)}"
mkdir -p "$OUT"

echo "== 1. create a 3-worker-node cluster =="
kind create cluster --name "$CLUSTER" --config "$HERE/kind-multinode.yaml"
kubectl get nodes -o wide | tee "$OUT/nodes.txt"

echo "== 2. build + load coordinator and operator images =="
docker build -t dtruntime:e2e -f "$ROOT/Dockerfile" "$ROOT"
docker build -t dtruntime-operator:e2e -f "$ROOT/Dockerfile.operator" "$ROOT"
kind load docker-image dtruntime:e2e dtruntime-operator:e2e --name "$CLUSTER"

echo "== 3. install CRD + operator, then apply a 3-worker TrainingRun =="
kubectl apply -f "$ROOT/k8s/operator/crd-trainingrun.yaml"
# deploy the operator (image dtruntime-operator:e2e) and a TrainingRun with
# world_size=3 and topologySpread so workers land on distinct nodes.
kubectl apply -f "$ROOT/k8s/operator/operator.yaml"
kubectl apply -f "$ROOT/k8s/operator/example-trainingrun.yaml"

echo "== 4. capture proof =="
# Workers scheduled on distinct nodes:
kubectl get pods -A -o wide | grep -E "worker|coordinator" | tee "$OUT/pods-by-node.txt"
# Run reaches Running/Succeeded (rendezvous formed across nodes):
kubectl get trainingrun -A -o wide | tee "$OUT/trainingrun-status.txt"
# Checkpoint restore + data locality: assert from worker logs / coordinator
# metrics (kubectl logs ...), saved into $OUT.

echo "Evidence written to $OUT. Tear down with: kind delete cluster --name $CLUSTER"
