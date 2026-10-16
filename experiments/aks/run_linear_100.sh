#!/usr/bin/env bash
# Run from the repository root after verifying Azure credentials and quotas.
set -euo pipefail
linear_counts=$(python3 -c 'print(",".join(str(n) for n in range(100, 9901, 100)))')
linear_output=${1:-artifacts/aks/linear-100-8cpu-westus2-20261014}
exec python3 experiments/aks/run_basic.py \
  --region westus2 --sku Standard_D4ls_v6 --vm-hourly-usd 0.179 --system-nodes 2 \
  --extra-user-pool d8as:Standard_D8as_v6:0.363 \
  --extra-user-pool d8s:Standard_D8s_v6:0.403 \
  --extra-user-pool d8ds:Standard_D8ds_v6:0.498 --dedicated-workload-nodes \
  --operator-cpu-request 1 --operator-memory-request 256Mi \
  --operator-cpu-limit 6 --operator-memory-limit 8Gi \
  --benchmark-script experiments/aks/response_gap.py \
  --benchmark-mode scale-until-unstable --namespace-counts "$linear_counts" \
  --scale-replicas 1,2 --repeat 1 \
  --benchmark-deadline-seconds 7200 --benchmark-timeout 7380 --wall-time-seconds 8400 \
  --output "$linear_output"
