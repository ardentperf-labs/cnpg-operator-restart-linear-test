# AKS linear namespace scaling: 100, 200, 300, …

On 2026-10-14 the AKS screen stopped at **700 watched namespaces, two operator replicas**, when a warm rolling restart failed to recover Cluster reconciliation workers within the configured **120-second deadline**. Both configurations completed scope updates and warm restarts through **600 namespaces**. At 700, both scope updates and the one-replica warm restart completed; the two-replica warm restart failed. No 800-or-higher level ran.

There were 28 measured trials: one scope update and one warm restart per namespace/replica combination, with no repetitions. This is a screen for the first operational failure, not a statistical estimate or a hard supported-namespace limit.

## Quantified restart gaps

Workerless time is the maximum continuous interval with **no Cluster-controller workers**, derived from the union of per-Pod `Starting workers`→`All workers finished` log intervals. Other CNPG controllers can be running during that interval. Trigger-to-recovery includes rollout scheduling, leader election, startup, and the final Deployment readiness check.

| Namespaces | Replicas | Log gap max / total (s) | Label max completed (s) | Pending label ≥ (s) | Baseline median / p95 / max (s) | Baseline n |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 1 | 13.240 / 13.240 | 13.357 | 0.000 | 0.360 / 0.526 / 0.526 | 8 |
| 100 | 2 | 11.036 / 11.036 | 14.912 | — | 0.358 / 0.604 / 0.604 | 8 |
| 200 | 1 | 23.590 / 23.590 | 24.548 | — | 0.387 / 0.483 / 0.483 | 8 |
| 200 | 2 | 22.944 / 22.944 | 23.465 | 0.070 | 0.355 / 0.392 / 0.392 | 9 |
| 300 | 1 | 33.483 / 33.483 | 33.824 | — | 0.475 / 0.503 / 0.503 | 7 |
| 300 | 2 | 33.884 / 33.884 | 34.715 | 0.000 | 0.487 / 0.545 / 0.545 | 7 |
| 400 | 1 | 41.922 / 41.922 | 48.044 | 0.000 | 0.502 / 0.615 / 0.615 | 7 |
| 400 | 2 | 44.454 / 44.454 | 45.330 | 0.160 | 0.483 / 0.521 / 0.521 | 8 |
| 500 | 1 | 59.979 / 59.979 | 61.146 | 0.000 | 0.498 / 0.834 / 0.834 | 7 |
| 500 | 2 | 54.756 / 54.756 | 57.300 | 0.420 | 0.504 / 0.716 / 0.716 | 7 |
| 600 | 1 | 64.753 / 64.753 | 68.336 | 0.315 | 0.458 / 0.534 / 0.534 | 8 |
| 600 | 2 | 83.224 / 91.208 | 91.097 | 0.000 | 0.523 / 0.873 / 0.873 | 7 |
| 700 | 1 | 99.454 / 99.454 | 104.027 | 0.525 | 0.461 / 0.497 / 0.497 | 7 |
| 700 | 2 | ≥108.084; censored | 0.825 | 119.719 | 0.595 / 0.825 / 0.825 | 6 |

At 600 with two replicas, the warm rollout had two separate workerless gaps, **7.98 and 83.22 seconds**, totaling **91.21 seconds**. The table gives the longer continuous gap, not the sum. For 700 with two replicas there was no captured replacement Cluster worker start before the trial ended; ≥108.08 seconds is a covered right-censored lower bound, not a completed duration or a zero gap.

The one-replica warm restart's maximum admitted-action latency rose from **13.36 seconds at 100** to **104.03 seconds at 700**. With two replicas at 600 it reached **91.10 seconds**. During the final failed restart, an action accepted at 06:05:18.994 UTC remained unapplied for **at least 119.72 seconds** when collection stopped; the gap since the previous completed action was at least **120.23 seconds**. The failure was the worker-recovery timeout, not a completed 120-second action timeout. Pod/Deployment readiness alone was insufficient: the failure snapshot had both replacement Pods Ready, with no container restarts, but no fresh Cluster worker marker.

Cross-node log timestamps may include node clock skew. Treat the exact boundaries as approximate; the large gaps are independently corroborated by controller-managed database-Pod label propagation. These probes were nonoverlapping, so they measure action lag without manufacturing a sustained backlog.

## Normal label latency and baseline limits

The same label probe ran before every trigger, for at least five seconds and
three completed actions. Eligible baseline actions completed before the trigger:
`phase_at_acceptance == baseline`, completed status, and `crossed_trigger == false`.
The table retains each trial's baseline median, nearest-rank p95, maximum and n.

Across the **104 pre-warm-restart actions**, normal API-acceptance→all-three-Pod
label propagation had a **0.459-second median**, **0.646-second p95**, and
**0.305–0.873-second range**. These are pooled action-latency samples, not
restart-latency percentiles; per-trial baselines are short.

Pre-scope-update baselines are separate: 94 completed actions, median 0.453 s,
p95 0.793 s, range 0.304–47.685 s. Three outliers before the one-replica updates
to 300, 500 and 600 namespaces lasted 18.071, 39.261 and 47.685 seconds. These
preceded their measured scope-update triggers and followed setup/replica changes;
this is not a dedicated, long-running no-restart control. Do not hide those tails
or attribute them to the subsequent trigger. [The scope-update table](../results/linear-2026-10-14/timings.md#namespace-scope-updates)
and [per-action measurements](../results/linear-2026-10-14/measurements.json) preserve them.

## Label probe methodology

Each action PATCHed a unique inherited label onto the PostgreSQL Cluster. The
probe measured client-monotonic time from receipt of a successful API response
to watch observation of the same label on **all three Ready database Pods**.
API request/admission time was recorded separately. The next action was sent
only after the preceding one completed, with a 0.25-second minimum interval, so
later updates could not overwrite an unobserved probe. Collection ran before,
during and after the disruption. Label latency includes API/watch observation
overhead and measures response to that action; it is not a direct worker-presence
measurement. The tables show the largest completed label latency across the
whole trial and retain pending ages separately. The failed trial's 0.825-second
completed maximum belongs to earlier successful actions; its **≥119.719-second
pending action** describes the stall.

Log timing instead uses `kubectl logs --timestamps` from every operator Pod,
identified by UID. The analyzer filters specifically for the Cluster controller,
builds each Pod's worker-active interval, and unions intervals across replicas.
Uncovered intervals are workerless gaps. It reports exact completed intervals
separately from a tail lacking a replacement start marker; a lower bound requires
log and Pod-watch coverage. `All workers finished` excludes old-worker drain time;
shutdown-context cancellation and drain-aware handoff are recorded separately.
Workers existing does not guarantee useful progress, which is why the label
probe provides an independent behavioral check. Readiness or a leader lease
alone did not count as reconciliation recovery.

## What lengthened the restart?

The dominant scalable term remains **watched namespace count**, even though almost all namespaces were empty. Database Cluster count stayed at one with three instances, images were cached, and the operator had a six-CPU/eight-GiB limit on eight-vCPU/32-GiB workload nodes.

A source-level explanation consistent with these results is that controller-runtime's multi-namespace informer lookup visits namespaces sequentially, and newly added informers in an already-started cache synchronously await client-go's 100 ms synchronization polling. The measured leader-lease→Cluster-worker startup component was **60.70 seconds at 600 namespaces** and **70.70 seconds at 700** for the one-replica warm restarts. That comparison uses each replacement Pod's own log clock and closely matches an additional ten seconds per hundred namespaces. It is evidence consistent with the source mechanism, not a trace proving that every delay was synchronization polling.

Leader takeover adds significant variation. At 600 with one replica, lease acquisition followed old-worker completion by **4.06 seconds**; at 700 it took **28.75 seconds**. Thus the ~34.7-second increase in total workerless time consists of roughly **24.7 extra seconds before lease acquisition** and **10.0 extra seconds afterward**, rather than slower draining. Old-worker drain in those two cases was under two milliseconds. Two replicas did not remove the delay when rolling replacement retired the old leader before a replacement's Cluster workers started.

Memory and goroutines also increased: one-replica post-window RSS grew from about **355 MiB at 100**, **1,009 MiB at 400**, to **1,646 MiB at 700**; goroutines rose from about **27,462** to **190,796**. The latter RSS is ~1.61 GiB, below the eight-GiB operator limit. These are sparse process-metric snapshots, not continuous cgroup peak measurements. The failed replacement's memory was not sampled after recovery because it did not recover. No operator container restart or OOM was observed.

In the failed two-replica restart, old Cluster workers finished at **06:05:30.768 UTC**. The replacement leader acquired the lease at **06:05:53.023**, about **22.26 seconds** later, and logged Cluster EventSources starting, but never logged Cluster workers starting before the **06:07:18.852** cutoff. The other replacement Pod only attempted leadership. That standby logged 52 HTTP/2 `client connection lost` watch-ended warnings across seven kinds at 06:06:13; the replacement leader logged none. This is a follower-side watch disruption, not evidence that the leader failed to acquire its lease or proof of API overload. The leader had approximately 85.83 seconds after lease acquisition before the cutoff without an observed Cluster worker start. Connectivity errors make it inappropriate to call this a pure CPU, memory, or cache-startup capacity limit. Repeating near 600–700 and measuring API/watch connection health would be needed to estimate failure probability or distinguish reproducible startup delay from a transient connectivity event.

## Database and admission impact

All **1,095 database health samples** were healthy. The same three database Pod UIDs remained Ready, with zero container restarts. All **2,893 acknowledged SQL writes** persisted, and the copied SQL log contained no failed attempts. The later table export contained three additional rows; the log and table snapshots are not atomic. `sql-validation.json` records the comparison.

There were **two transient HTTP 500 admission rejections**, both webhook EOF errors. One occurred during the baseline before the first 3→100 scope-update trigger; it cannot be assigned to that restart. The other occurred during the 100→200 transition, near Pod replacement. The following actions succeeded. The artifacts do not include webhook endpoint telemetry sufficient to prove a root cause. These were retained as errors but did not meet the screen's sustained recovery/action failure condition.

The experiment did not kill a database primary, so it does not demonstrate a failover outage. The observed absence of Cluster workers and long action lag establish a period during which operator-driven handling of a database failure could be delayed. Existing PostgreSQL service continued handling the SQL probes throughout this run.

## Fixture, reproduction, and cleanup

The West US 2 fixture used: CNPG **1.30.1**, PostgreSQL **18.6**, version tags pinned to image digests, `IfNotPresent`, image prewarming, one three-instance synchronous PostgreSQL Cluster, and the same operator resources. Three User nodes (`Standard_D8as_v6`, `Standard_D8s_v6`, `Standard_D8ds_v6`) each had eight vCPUs/32 GiB; two four-vCPU `Standard_D4ls_v6` System nodes hosted system workloads. Required hostname separation and a workload selector confined the operator and databases to User nodes. Different User SKUs remain a confound for small timing differences. East US workloads were untouched.

The fixture bootstrapped with three watched namespaces. The first measured level included 3→100 updates for each replica count; later measured updates were 100→200, 200→300, etc. Between paired cases the operator returned to the previous successful scope. Each successful recovery was followed by at least 20 seconds and three distinct completed label actions. The worker and action limits were fixed at 120 seconds. The general `--recovery-timeout` metadata defaults to 180 seconds, but scale mode explicitly overrides the worker-recovery gate to 120; the final failure message confirms that enforced limit. Its last action was censored just short of its own 120-second timeout.

Reproduction (see [requirements and cleanup behavior](../experiments/aks/README.md)):

```bash
bash experiments/aks/run_linear_100.sh artifacts/aks/linear-100-8cpu-westus2-20261014
```

The script's count list is 100..9,900 in steps of 100, but the run stopped at its first failure at 700. It has a two-hour benchmark safety deadline, a 140-minute provisioning/testing guard, and separate bounded deletion verification. Safety-deadline exhaustion is distinguished from an operator failure. Prices/quotas must be checked again before reuse.

The initial credential preflight was blocked by an expired service-principal secret and created no resources. After reconnection the run began provisioning at **05:01:53 UTC**, and the database/benchmark became ready at **05:16:58**. The benchmark ended at **06:07:18**; evidence export finished and resource-group deletion began at **06:07:21**, about three seconds later. The uniquely owned groups are `cnpg-restart-20261014-b6907f86` and `cnpg-restart-20261014-b6907f86-nodes`. Both groups were verified absent at **06:15:34 UTC**; the node resource group was already confirmed absent at 06:10:46. The lifecycle ledger estimates **$1.99 as a conservative VM-compute upper bound**, applying all five rates over the full 73.68-minute provisioning-through-deletion interval. This excludes disks/networking and is not a billed-cost report. The rates were rechecked against Azure Retail Prices before the run.

The shareable [measurements](../results/linear-2026-10-14/measurements.json) contain all label observations, worker-gap intervals, baseline statistics, source-file hashes, and the cleanup/SQL summaries. Full raw and derived data are retained locally under `artifacts/aks/linear-100-8cpu-westus2-20261014/`, ignored by Git: action JSONL, per-UID logs and Pod-watch events, benchmark results, worker-gap analysis, final scale summaries, SQL/persistence audit, node/Pod placement, price snapshots, and lifecycle ledger. Published analysis uses `analyze_worker_gaps.py` followed by `export_linear_results.py`; both operate offline.

The local kind fixture was paused before all benchmark cases to free collector memory. After cloud cleanup, it was restored and its node and both existing PostgreSQL Clusters verified healthy.

Primary pinned dependency source for the startup hypothesis:

- [controller-runtime v0.25.1 multi-namespace informer loop](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.1/pkg/cache/multi_namespace_cache.go#L101).
- [Default blocking cache synchronization](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.1/pkg/cache/internal/informers.go#L322).
- [Event-source informer request](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.1/pkg/internal/source/kind.go#L88).
- [client-go v0.37.0 100 ms synchronization polling](https://github.com/kubernetes/client-go/blob/v0.37.0/tools/cache/shared_informer.go#L491).
