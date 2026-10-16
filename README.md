# CNPG operator restart: linear namespace scaling on AKS

## Methodology

We increased the operator's explicit watch list from **100 namespaces in steps
of 100**, testing **one and two operator replicas** at each level. Each configuration
had one measured scope update and one warm rolling restart. The fixture held
database count constant: one PostgreSQL Cluster with three instances. Workload
nodes had eight vCPUs, and images were already cached.

We measured two distinct intervals:

- **Log-based reconciliation gap:** timestamped `Starting workers` and
  `All workers finished` messages from every operator Pod defined active
  Cluster-controller intervals. We combined intervals across replicas and
  measured periods with no active Cluster workers. The table shows maximum
  continuous gap and total gap separately. Pod readiness and acquiring the
  leader lease did not count as worker recovery.
- **Label-based action latency:** repeatedly PATCH a unique inherited label on
  the PostgreSQL Cluster, then measure client-observed API acceptance to that
  label appearing on **all three Ready database Pods**. Wait for each action
  before sending another so later updates cannot hide an unfinished action.
  This independently checks useful reconciliation.

The same label probe ran before each trigger as a **baseline control**. Across
**104 completed pre-warm-restart actions**, baseline latency was **0.459 s median**,
**0.646 s p95**, and **0.305–0.873 s range**. P95 uses nearest rank. These are
short within-trial controls, not a separate long-running control experiment.
Pre-scope-update baselines are reported separately and retain three long outliers.

Each successful trial collected at least 20 seconds and three completed actions
after worker recovery. The screen stopped at its first 120-second worker-recovery
or action failure. Incomplete intervals are reported as **lower bounds**.
Cross-node clock skew can affect log boundaries; label latency includes client
API/watch observation overhead. No database primary was killed, so this measures
operator reconciliation interruption rather than failover downtime.

## Results

**The sweep stopped at 700 namespaces during the two-replica warm restart.**
Both configurations completed through 600. At 700, the one-replica warm restart
and both scope updates completed, but the two-replica warm restart exceeded its
120-second worker-recovery limit. This is one observation per case, not a proven
hard namespace limit or a restart-latency percentile estimate.

All timing columns below are **seconds**. Label max is the largest completed
action latency across the trial; pending label age is shown separately.

### 1 replica

| Namespaces | Log gap max / total (s) | Label max completed (s) | Pending label ≥ (s) | Baseline median / p95 / max (s) | Baseline n |
|---:|---:|---:|---:|---:|---:|
| 100 | 13.240 / 13.240 | 13.357 | 0.000 | 0.360 / 0.526 / 0.526 | 8 |
| 200 | 23.590 / 23.590 | 24.548 | — | 0.387 / 0.483 / 0.483 | 8 |
| 300 | 33.483 / 33.483 | 33.824 | — | 0.475 / 0.503 / 0.503 | 7 |
| 400 | 41.922 / 41.922 | 48.044 | 0.000 | 0.502 / 0.615 / 0.615 | 7 |
| 500 | 59.979 / 59.979 | 61.146 | 0.000 | 0.498 / 0.834 / 0.834 | 7 |
| 600 | 64.753 / 64.753 | 68.336 | 0.315 | 0.458 / 0.534 / 0.534 | 8 |
| 700 | 99.454 / 99.454 | 104.027 | 0.525 | 0.461 / 0.497 / 0.497 | 7 |

### 2 replicas

| Namespaces | Log gap max / total (s) | Label max completed (s) | Pending label ≥ (s) | Baseline median / p95 / max (s) | Baseline n |
|---:|---:|---:|---:|---:|---:|
| 100 | 11.036 / 11.036 | 14.912 | — | 0.358 / 0.604 / 0.604 | 8 |
| 200 | 22.944 / 22.944 | 23.465 | 0.070 | 0.355 / 0.392 / 0.392 | 9 |
| 300 | 33.884 / 33.884 | 34.715 | 0.000 | 0.487 / 0.545 / 0.545 | 7 |
| 400 | 44.454 / 44.454 | 45.330 | 0.160 | 0.483 / 0.521 / 0.521 | 8 |
| 500 | 54.756 / 54.756 | 57.300 | 0.420 | 0.504 / 0.716 / 0.716 | 7 |
| 600 | 83.224 / 91.208 | 91.097 | 0.000 | 0.523 / 0.873 / 0.873 | 7 |
| 700 | ≥108.084; censored | 0.825 | 119.719 | 0.595 / 0.825 / 0.825 | 6 |

In the failed 700/two-replica case, the **0.825-second completed maximum covers
earlier actions only**. An accepted label remained unapplied for **at least
119.719 seconds** when collection ended, and there were no Cluster workers for
**at least 108.084 seconds**. At 600/two replicas, the two distinct log gaps
were 7.984 and 83.224 seconds; the total was 91.208 seconds.

The measurements show roughly increasing startup delay with namespace count,
with additional leader-election variation. All **1,095 database health samples**
were healthy; all **2,893 acknowledged SQL writes** persisted, with zero logged
SQL failures or database Pod replacements/restarts. Two transient webhook EOF
rejections were retained in the action data.

Both Azure experiment resource groups were **verified deleted** on 2026-10-14.
The conservative VM-compute upper bound was **$1.99**, excluding disks/networking.

## Two-replica configuration

Two replicas meant **two Pods of the same CNPG operator Deployment**, both
configured with the same namespace watch list. PostgreSQL still had three
instances; changing the operator replica count did not add databases or divide
the namespaces between operators.

- **Leadership:** the release manifest enabled `--leader-elect`. The Pods
  competed for the same Kubernetes Lease, `cnpg-system/db9c8771.cnpg.io`.
  Only the elected leader ran the Cluster reconciliation workers; the other
  Pod was a standby that could take over. Some caches could already be running
  on the standby, but becoming leader still required controller/event-source
  startup and synchronization.
- **Placement and resources:** required hostname anti-affinity placed the two
  operator Pods on different 8-vCPU workload nodes. Each operator requested
  one CPU/256 MiB and had limits of six CPUs/eight GiB.
- **Rolling replacement:** `RollingUpdate`, `maxSurge: 1`, `maxUnavailable: 0`.
  A restart replaced both operator Pods, permitting three operator Pods
  temporarily across the three workload nodes. The scope update also rolled
  both Pods because it changed their `WATCH_NAMESPACE` environment variable.
- **Readiness versus reconciliation:** Kubernetes could mark a replacement
  Ready before its Cluster workers started. Consequently, it could retire the
  old leader while the replacement was still acquiring leadership or
  synchronizing controller sources. A rollout could have multiple handoffs
  and separate workerless gaps, despite maintaining Ready Pods.

The two-replica table therefore measures rolling replacement of the operator
Deployment with a standby present. It does not measure deleting only the leader
while leaving a stable standby untouched. The second replica provides a
leadership takeover candidate; it did not guarantee uninterrupted reconciliation
in these rolling-restart trials.

## Data and reproduction

- [Detailed methodology, baseline caveats and interpretation](docs/aks-linear-namespace-scaling-results.md)
- [All 28 timing rows, including namespace-scope updates](results/linear-2026-10-14/timings.md)
- [Measurements and per-action observations](results/linear-2026-10-14/measurements.json)
- [Requirements, reproducible launch and automatic cleanup](experiments/aks/README.md)

This repository contains only the **2026-10-14 linear experiment** and its
supporting harness. Earlier experiments and upstream source checkouts are excluded.
