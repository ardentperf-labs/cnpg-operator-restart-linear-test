# Linear test timing tables

All times are seconds. Log gaps measure absence of Cluster-controller workers; label times measure API acceptance → matching labels on all three database Pods. They are different intervals.

Baseline uses completed pre-trigger actions that did not cross the trigger. All baseline outliers are retained. P95 uses nearest rank; per-case sample counts are small.

A pending label age is a censored lower bound, not a completed latency. Small pending ages can be the final action cut off at the end of an otherwise successful collection window. In the failed 700/two-replica trial, the subsecond completed maximum covers only actions that finished before the stall; the pending age is the relevant bound.

## Warm rolling restarts

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

## Namespace-scope updates

Scope updates start at the preceding count (the first is 3→100). The displayed count is the target.

### 1 replica

| Namespaces | Log gap max / total (s) | Label max completed (s) | Pending label ≥ (s) | Baseline median / p95 / max (s) | Baseline n |
|---:|---:|---:|---:|---:|---:|
| 100 | 11.275 / 11.275 | 13.133 | 0.000 | 0.424 / 0.522 / 0.522 | 6 |
| 200 | 23.289 / 23.289 | 23.495 | 0.063 | 0.339 / 0.510 / 0.510 | 8 |
| 300 | 34.061 / 34.061 | 35.060 | — | 0.426 / 18.071 / 18.071 | 4 |
| 400 | 45.049 / 45.049 | 45.400 | 0.320 | 0.465 / 0.558 / 0.558 | 7 |
| 500 | 53.192 / 53.192 | 54.125 | — | 0.634 / 39.261 / 39.261 | 4 |
| 600 | 65.368 / 65.368 | 69.480 | 0.051 | 0.670 / 47.685 / 47.685 | 4 |
| 700 | 99.849 / 99.849 | 105.832 | 0.000 | 0.524 / 0.563 / 0.563 | 7 |

### 2 replicas

| Namespaces | Log gap max / total (s) | Label max completed (s) | Pending label ≥ (s) | Baseline median / p95 / max (s) | Baseline n |
|---:|---:|---:|---:|---:|---:|
| 100 | 10.962 / 11.802 | 11.885 | — | 0.419 / 0.454 / 0.454 | 8 |
| 200 | 22.920 / 29.876 | 30.975 | 0.189 | 0.364 / 0.438 / 0.438 | 9 |
| 300 | 32.443 / 39.489 | 40.546 | 0.324 | 0.442 / 0.484 / 0.484 | 8 |
| 400 | 44.819 / 44.819 | 46.171 | 0.128 | 0.488 / 0.842 / 0.842 | 7 |
| 500 | 56.425 / 56.425 | 56.881 | — | 0.491 / 0.573 / 0.573 | 7 |
| 600 | 83.633 / 91.689 | 94.873 | 0.223 | 0.423 / 0.490 / 0.490 | 8 |
| 700 | 93.951 / 102.017 | 108.542 | 0.436 | 0.507 / 0.793 / 0.793 | 7 |

At 600/two replicas the warm restart had separate log gaps of 7.984 and 83.224 seconds. Max and total are shown separately.
