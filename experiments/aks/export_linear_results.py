#!/usr/bin/env python3
"""Export shareable measurements and both timing tables from one linear run."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics


def stats(values):
    values = sorted(values)
    if not values:
        return {'n': 0, 'median': None, 'p95': None, 'min': None, 'max': None}
    return {'n': len(values), 'median': statistics.median(values),
            'p95': values[math.ceil(.95 * len(values)) - 1],
            'min': values[0], 'max': values[-1]}


def baseline(actions):
    return [a['action_latency_seconds'] for a in actions
            if a.get('status') == 'completed'
            and a.get('phase_at_acceptance') == 'baseline'
            and not a.get('crossed_trigger')]


def export(root):
    inputs = {'benchmark': root / 'benchmark/results.json',
              'workers': root / 'worker-gaps.json',
              'sql': root / 'sql-validation.json', 'lifecycle': root / 'lifecycle.json'}
    data = {key: json.loads(path.read_text()) for key, path in inputs.items()}
    workers = {Path(t['event_file']).stem: t for t in data['workers']['trials']}
    rows = []
    action_keys = ('number', 'status', 'accepted', 'accepted_utc', 'completed_utc_observed',
                   'phase_at_acceptance', 'phase_at_completion', 'crossed_trigger',
                   'action_latency_seconds', 'admission_seconds', 'pending_age_seconds')
    for trial in data['benchmark']['trials']:
        worker = workers[trial['id']]
        gaps = worker['worker_active_interval_union']['zero_worker_gaps_wholly_inside_trial']
        tail = worker['worker_gap_interpretation'].get('censored_zero_worker_tail') or {}
        lower = tail.get('lower_bound_seconds')
        actions = [{key: action[key] for key in action_keys if key in action}
                   for action in trial['actions']]
        recovery = trial.get('recovery_monotonic_ns')
        rows.append({'id': trial['id'], 'namespaces': trial['namespace_count'],
                     'previous_namespaces': trial.get('previous_namespace_count'),
                     'replicas': trial['desired_replicas'], 'scenario': trial['scenario'],
                     'status': trial['status'], 'trigger_utc': trial.get('trigger_utc'),
                     'finished_utc': trial.get('finished_utc'),
                     'recovery_seconds': ((recovery-trial['trigger_monotonic_ns'])/1e9
                                          if recovery else None),
                     'log_gap_seconds': [g['seconds'] for g in gaps],
                     'log_gap_intervals': [{k: g[k] for k in ('start_utc', 'end_utc', 'seconds')}
                                           for g in gaps],
                     'log_gap_max_seconds': max((g['seconds'] for g in gaps), default=None),
                     'log_gap_total_seconds': sum(g['seconds'] for g in gaps) if gaps else None,
                     'log_gap_lower_bound_seconds': lower,
                     'log_gap_censored_start_utc': tail.get('start_utc'),
                     'log_gap_censored_cutoff_utc': tail.get('cutoff_utc'),
                     'baseline': stats(baseline(actions)),
                     'max_completed_label_seconds': max((a['action_latency_seconds'] for a in actions
                                                         if a.get('status') == 'completed'), default=None),
                     'max_pending_label_age_seconds': max((a['pending_age_seconds'] for a in actions
                                                          if 'pending_age_seconds' in a), default=None),
                     'label_actions': actions})
    life = data['lifecycle']
    sql = data['sql']
    return {'schema_version': 1, 'run_date': '2026-10-14',
            'source_sha256': {k: hashlib.sha256(p.read_bytes()).hexdigest() for k, p in inputs.items()},
            'baseline_definition': 'Completed, accepted during baseline, and did not cross the trigger; p95 uses nearest rank.',
            'label_definition': 'Client-observed API acceptance to the matching inherited label on all three Ready database Pods; max includes all completed phases.',
            'warm_restart_baseline': stats([v for t in rows if t['scenario'] == 'rollout-restart'
                                            for v in baseline(t['label_actions'])]),
            'scope_update_baseline': stats([v for t in rows if t['scenario'] == 'namespace-scope-update'
                                            for v in baseline(t['label_actions'])]),
            'cleanup': {k: life.get(k) for k in ('cleanup_verified', 'cleanup_started_utc',
                        'cleanup_completed_utc', 'vm_compute_upper_bound_usd')},
            'sql': sql['sql'],
            'database': {k: sql['database'][k] for k in ('same_pod_uids','health_samples','unhealthy_samples')},
            'trials': rows}


def fmt(value):
    return '—' if value is None else f'{value:.3f}'


def table(rows):
    lines = ['| Namespaces | Replicas | Log gap max / total (s) | Label max completed (s) | Pending label ≥ (s) | Baseline median / p95 / max (s) | Baseline n |',
             '|---:|---:|---:|---:|---:|---:|---:|']
    for t in rows:
        gap = (f"≥{fmt(t['log_gap_lower_bound_seconds'])}; censored"
               if t['log_gap_lower_bound_seconds'] is not None
               else f"{fmt(t['log_gap_max_seconds'])} / {fmt(t['log_gap_total_seconds'])}")
        b = t['baseline']
        lines.append(f"| {t['namespaces']} | {t['replicas']} | {gap} | {fmt(t['max_completed_label_seconds'])} | {fmt(t['max_pending_label_age_seconds'])} | {fmt(b['median'])} / {fmt(b['p95'])} / {fmt(b['max'])} | {b['n']} |")
    return '\n'.join(lines)


def markdown(data):
    lines = ['# Linear test timing tables', '',
             'All times are seconds. Log gaps measure absence of Cluster-controller workers; label times measure API acceptance → matching labels on all three database Pods. They are different intervals.', '',
             'Baseline uses completed pre-trigger actions that did not cross the trigger. All baseline outliers are retained. P95 uses nearest rank; per-case sample counts are small.', '',
             'A pending label age is a censored lower bound, not a completed latency. Small pending ages can be the final action cut off at the end of an otherwise successful collection window. In the failed 700/two-replica trial, the subsecond completed maximum covers only actions that finished before the stall; the pending age is the relevant bound.', '',
             '## Warm rolling restarts', '',
             table([t for t in data['trials'] if t['scenario']=='rollout-restart']), '',
             '## Namespace-scope updates', '',
             'Scope updates start at the preceding count (the first is 3→100). The displayed count is the target.', '',
             table([t for t in data['trials'] if t['scenario']=='namespace-scope-update']), '',
             'At 600/two replicas the warm restart had separate log gaps of 7.984 and 83.224 seconds. Max and total are shown separately.', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory', type=Path)
    parser.add_argument('output_directory', type=Path)
    args = parser.parse_args()
    data = export(args.run_directory)
    args.output_directory.mkdir(parents=True, exist_ok=True)
    (args.output_directory/'measurements.json').write_text(json.dumps(data, indent=2)+'\n')
    (args.output_directory/'timings.md').write_text(markdown(data))
    print(f"Exported {len(data['trials'])} trials to {args.output_directory}")


if __name__ == '__main__':
    main()
