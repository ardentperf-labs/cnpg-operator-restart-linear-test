# Reproduce the linear AKS test

Prerequisites: Linux, Python 3.10+, Azure CLI, kubectl, and Azure credentials
permitted to create/delete resource groups and AKS clusters. Python uses only
the standard library. Run commands from the repository root.

Check the active subscription, West US 2 SKU availability, quota and rates before
launch. The recorded fixture used all 32 regional vCPUs: two D4ls_v6 System
nodes and one each of D8as_v6, D8s_v6 and D8ds_v6 User nodes. Each family used
eight cores. Operator and database Pods were constrained to the three 8-vCPU
User nodes. Prices embedded in the launch script are the measured-run rates,
not a guarantee of current prices. East US is not used.

```bash
bash experiments/aks/run_linear_100.sh artifacts/linear-new-run
```

Use a new output directory per run. This provisions paid Azure resources. The
runner creates uniquely named groups, writes their names to `lifecycle.json`,
exports evidence, starts deletion immediately after the benchmark, and verifies
that both owned groups are absent. Kubeconfig is held in a private temporary
directory and removed after cleanup. Existing clusters are not modified.
If the runner is interrupted, inspect the ledger and verify deletion of its
exact resource-group names before leaving it unattended.

The fixture uses CNPG 1.30.1 and PostgreSQL 18.6 images pinned to the recorded
digests in `run_basic.py`; it no longer depends on the original machine's
`/tmp` registry-manifest files. The SQL probe manifest is included beside the
runner and obtains passwords from Kubernetes Secret references. No credentials
are included in the published measurements. The CloudNativePG release manifest
is downloaded during setup.

The fixture initializes at three namespaces. The measured first step is 3→100;
then 100→200, 200→300, etc. At each level it measures the scope update and one
warm rolling restart, first with one replica and then with two. Between paired
cases it resets to the preceding successful namespace scope. The count list
ends at 9,900, but the first controller/action failure stops the whole screen.
One observation per case is not a percentile estimate of restart downtime.

Scale mode enforces a **120-second worker-recovery gate**, overriding the general
180-second `--recovery-timeout` default recorded in metadata. Action timeout is
also 120 seconds. Success requires final Pod readiness and a fresh Cluster
worker-start log marker, then at least 20 seconds and three completed probes.
The benchmark safety deadline is two hours; the provisioning/testing guard is
140 minutes. Cleanup verification has a separate bound. A safety-deadline stop
is classified separately from a controller failure.

After the runner exits, analyze its local artifacts without contacting Azure:

```bash
python3 experiments/aks/analyze_worker_gaps.py artifacts/linear-new-run
python3 experiments/aks/audit_sql.py artifacts/linear-new-run
python3 experiments/aks/export_linear_results.py artifacts/linear-new-run results/new-run
```

The SQL audit reads `sql-client.log`, `persisted-write-ids.txt`, before/after
database Pod snapshots and benchmark health samples. The log/table snapshots
are not atomic; extra persisted rows and timed-out attempts remain separate.
The log-gap analyzer itself needs no SQL audit.

The scripts also retain generic harness modes used during development; this
repository's documented launch command, results and scope are the linear test.
