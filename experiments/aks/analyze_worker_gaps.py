#!/usr/bin/env python3
"""Derive CNPG Cluster-controller worker stop/start gaps from a run directory."""
import argparse
import datetime
import json
from pathlib import Path
import re


MARKER_TEXT = {
    'starting_workers': 'Starting workers',
    'shutdown_signal': 'Shutdown signal received, waiting for all workers to finish',
    'all_workers_finished': 'All workers finished',
}
UID_PATTERN = re.compile(r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})')


def parse_time(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.datetime.fromtimestamp(value, datetime.timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value:
        return None
    if value.endswith('Z'):
        value = value[:-1] + '+00:00'
    try:
        result = datetime.datetime.fromisoformat(value)
    except ValueError:
        return None
    if result.tzinfo is None:
        return result.replace(tzinfo=datetime.timezone.utc)
    return result.astimezone(datetime.timezone.utc)


def iso(value):
    return value.isoformat() if value is not None else None


def load_jsonl(path):
    result = []
    if path is None or not path.exists():
        return result
    for line in path.read_text(encoding='utf-8', errors='replace').splitlines():
        try:
            result.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return result


def find_results(run_dir):
    candidates = [run_dir / 'benchmark' / 'results.json', run_dir / 'results.json']
    for path in candidates:
        if path.exists():
            data = json.loads(path.read_text(encoding='utf-8'))
            if isinstance(data, list):
                return path, data
            if isinstance(data, dict):
                trials = data.get('trials') or data.get('results')
                if isinstance(trials, list):
                    return path, trials
    raise FileNotFoundError('expected benchmark/results.json or results.json with a trial list')


def candidate_path(base_dir, value):
    if not value:
        return None
    path = Path(value)
    if path.is_absolute() and path.exists():
        return path
    for base in (base_dir, base_dir.parent):
        candidate = base / path
        if candidate.exists():
            return candidate
    return None


def uid_set(value):
    if isinstance(value, dict):
        return {str(uid) for uid in value.values() if uid}
    if isinstance(value, (list, tuple, set)):
        return {str(uid) for uid in value if uid}
    if isinstance(value, str):
        return {value}
    return set()


def first_value(row, names):
    for name in names:
        value = row.get(name)
        if value is not None:
            return value
    return None


def trial_context(row, results_path, run_dir):
    base = results_path.parent
    events_path = candidate_path(base, first_value(row, ['event_file', 'events_file']))
    events = load_jsonl(events_path)
    completion = next((e for e in reversed(events) if e.get('event') in ('trial_completed', 'trial_finished')), None)
    start_event = next((e for e in events if e.get('event') == 'trial_started'), None)
    trigger_time = parse_time(first_value(row, ['trigger_utc', 'trigger_started_utc', 'trigger_start_utc']))
    trigger_source = 'benchmark result'
    if trigger_time is None:
        trigger_name = next((name for name in ('leader_delete_requested', 'standby_delete_requested', 'rollout_restart_requested')
                             if any(e.get('event') == name for e in events)), None)
        trigger_event = next((e for e in events if e.get('event') == trigger_name), None) if trigger_name else None
        trigger_time = parse_time((trigger_event or {}).get('utc'))
        trigger_source = 'trigger request event; operation start may precede this by trigger API duration'
    finished_time = parse_time(first_value(row, ['finished_utc', 'finished_at_utc', 'completed_utc', 'trial_finished_utc']))
    if finished_time is None:
        finished_time = parse_time((completion or {}).get('utc'))
    if finished_time is None:
        finished_time = parse_time(first_value(row, ['utc_end', 'end_utc']))

    pre_uids = uid_set(first_value(row, ['initial_operator_pod_ids', 'preUIDs', 'pre_uids', 'pre_operator_uids']))
    final_uids = uid_set(first_value(row, ['final_operator_pod_ids', 'finalUIDs', 'final_uids', 'final_operator_uids']))
    prestate_path = candidate_path(base, first_value(row, ['prestate_file', 'prestate_path']))
    prestate = {}
    if prestate_path is not None:
        try:
            prestate = json.loads(prestate_path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            pass
    embedded = (start_event or {}).get('prestate') or {}
    for container in (prestate, embedded):
        for key in ('operator_pod_exact_ids', 'operator_pod_ids', 'initial_operator_pod_ids', 'preUIDs'):
            if not pre_uids:
                pre_uids = uid_set(container.get(key))
    result_row = (completion or {}).get('result') or {}
    if not final_uids:
        for source in (row, result_row):
            final_uids = uid_set(first_value(source, ['final_operator_pod_ids', 'finalUIDs', 'final_uids', 'final_operator_uids']))
            if final_uids:
                break
    return {
        'row': row, 'events': events, 'event_file': str(events_path) if events_path else None,
        'trigger': trigger_time, 'trigger_source': trigger_source,
        'finished': finished_time, 'pre_uids': pre_uids, 'final_uids': final_uids,
    }


def cluster_controller_name(obj):
    values = [obj.get('controller'), obj.get('controller_name'), obj.get('logger'), obj.get('name')]
    for value in values:
        if not isinstance(value, str):
            continue
        normalized = value.lower().strip()
        if normalized == 'cluster' or normalized.endswith('.cluster') or normalized.endswith('/cluster'):
            return value
    return None


def parse_log_line(line):
    line = line.rstrip('\r\n')
    if not line:
        return None
    prefix, separator, payload = line.partition(' ')
    timestamp = parse_time(prefix) if separator else None
    structured = None
    if separator:
        try:
            structured = json.loads(payload)
        except json.JSONDecodeError:
            pass
    if structured is None:
        try:
            structured = json.loads(line)
        except json.JSONDecodeError:
            structured = None
    if isinstance(structured, dict):
        message = structured.get('msg') or structured.get('message')
        if not isinstance(message, str):
            message = ''
        if timestamp is None:
            timestamp = parse_time(structured.get('ts') or structured.get('time') or structured.get('timestamp'))
        controller = cluster_controller_name(structured)
        worker_count = structured.get('worker count', structured.get('worker_count'))
        return timestamp, message, controller, worker_count
    if timestamp is not None:
        return timestamp, payload, None, None
    return None, line, None, None


def marker_kind(message):
    for kind, phrase in MARKER_TEXT.items():
        if phrase in message:
            return kind
    return None


def load_logs(run_dir):
    watch_rows = []
    for path in run_dir.rglob('operator-watch.jsonl'):
        watch_rows.extend(load_jsonl(path))
    pod_info = {}
    pod_watch = {}
    for event in watch_rows:
        uid = event.get('pod_uid')
        if uid:
            pod_info[uid] = {'pod_name': event.get('pod_name'), 'node': event.get('node')}
            entry = pod_watch.setdefault(uid, {
                'pod_uid': uid, 'pod_name': event.get('pod_name'), 'node': event.get('node'),
                'event_count': 0, 'event_types': [], 'first_received_utc': event.get('received_utc'),
                'last_received_utc': event.get('received_utc'), 'ever_ready': False,
                'manager_running_seen': False, 'deleted_seen': False,
            })
            entry['event_count'] += 1
            entry['pod_name'] = event.get('pod_name') or entry.get('pod_name')
            entry['node'] = event.get('node') or entry.get('node')
            entry['last_received_utc'] = event.get('received_utc') or entry.get('last_received_utc')
            event_type = event.get('type')
            if event_type and event_type not in entry['event_types']:
                entry['event_types'].append(event_type)
            entry['ready'] = event.get('ready')
            entry['ever_ready'] = entry['ever_ready'] or event.get('ready') is True
            entry['phase'] = event.get('phase')
            entry['creation_timestamp'] = event.get('creation_timestamp') or entry.get('creation_timestamp')
            entry['deletion_timestamp'] = event.get('deletion_timestamp')
            entry['deleted_seen'] = entry['deleted_seen'] or event_type == 'DELETED'
            containers = event.get('containers') or {}
            manager = containers.get('manager') or {}
            if manager.get('running_started_at'):
                entry['manager_running_seen'] = True
                entry['manager_running_started_at'] = manager['running_started_at']

    seen = set()
    markers = []
    unknown_controller_candidates = 0
    duplicate_lines = 0
    files_by_uid = {}
    log_paths = sorted(run_dir.rglob('*.log'))
    for path in log_paths:
        match = UID_PATTERN.search(path.name)
        uid = match.group(1).lower() if match else None
        if uid is None:
            continue
        files_by_uid.setdefault(uid, []).append(str(path))
        for line in path.read_text(encoding='utf-8', errors='replace').splitlines():
            parsed = parse_log_line(line)
            if parsed is None:
                continue
            timestamp, message, controller, worker_count = parsed
            kind = marker_kind(message)
            if kind is None:
                continue
            if controller is None:
                unknown_controller_candidates += 1
                continue
            key = (uid, iso(timestamp), message)
            if timestamp is not None:
                if key in seen:
                    duplicate_lines += 1
                    continue
                seen.add(key)
            markers.append({
                'pod_uid': uid, 'pod_name': pod_info.get(uid, {}).get('pod_name'),
                'node': pod_info.get(uid, {}).get('node'), 'timestamp': timestamp,
                'timestamp_utc': iso(timestamp), 'marker': kind,
                'message': message, 'controller': controller,
                'worker_count': worker_count,
            })
    markers.sort(key=lambda row: (row['timestamp'] or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
                                  row['pod_uid'], row['marker']))
    return (markers, files_by_uid, duplicate_lines, unknown_controller_candidates,
            len(log_paths), pod_watch)


def build_intervals(markers):
    by_uid = {}
    for marker in markers:
        by_uid.setdefault(marker['pod_uid'], []).append(marker)
    intervals = []
    for uid, events in by_uid.items():
        active_start = None
        pending_shutdown = None
        for event in sorted(events, key=lambda row: row['timestamp'] or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)):
            if event['timestamp'] is None:
                continue
            if event['marker'] == 'starting_workers':
                if active_start is None:
                    active_start = event
                    pending_shutdown = None
                else:
                    # Preserve overlapping/repeated starts as a coverage anomaly, not a fake new interval.
                    event['duplicate_start_while_active'] = True
            elif event['marker'] == 'shutdown_signal':
                if pending_shutdown is None:
                    pending_shutdown = event
            elif event['marker'] == 'all_workers_finished':
                intervals.append({
                    'pod_uid': uid,
                    'start': active_start.get('timestamp') if active_start else None,
                    'end': event['timestamp'],
                    'start_event': active_start,
                    'shutdown_event': pending_shutdown,
                    'finish_event': event,
                    'open_ended': False,
                })
                active_start = None
                pending_shutdown = None
        if active_start is not None:
            intervals.append({
                'pod_uid': uid, 'start': active_start['timestamp'], 'end': None,
                'start_event': active_start, 'shutdown_event': pending_shutdown,
                'finish_event': None, 'open_ended': True,
            })
    return intervals


def seconds_between(later, earlier):
    return round((later - earlier).total_seconds(), 3) if later is not None and earlier is not None else None


def analyze_trial(context, markers, intervals, files_by_uid, pod_watch):
    row = context['row']
    begin, end = context['trigger'], context['finished']
    trial_name = row.get('scenario') or row.get('case') or row.get('name')
    trial_no = row.get('trial') or row.get('repeat')
    if begin is None or end is None:
        window_markers = []
        errors = ['missing trigger or finished UTC; cannot bound worker gaps to the trial']
    else:
        window_markers = [m for m in markers if m['timestamp'] is not None and begin <= m['timestamp'] <= end]
        errors = []
    pre_uids = context['pre_uids']
    final_uids = context['final_uids']
    expected_uids = pre_uids | final_uids
    missing_uid_logs = sorted(uid for uid in expected_uids if uid not in files_by_uid)
    old_shutdowns = [m for m in window_markers if m['marker'] == 'shutdown_signal' and
                     (not pre_uids or m['pod_uid'] in pre_uids)]
    starts_after = [m for m in window_markers if m['marker'] == 'starting_workers']
    finished = [m for m in window_markers if m['marker'] == 'all_workers_finished']
    new_pod_uids = set()
    new_pod_watch = []
    for uid, info in pod_watch.items():
        if uid in pre_uids:
            continue
        created = parse_time(info.get('creation_timestamp'))
        first_seen = parse_time(info.get('first_received_utc'))
        last_seen = parse_time(info.get('last_received_utc'))
        in_trial = (uid in final_uids or
                    (begin is not None and end is not None and
                     ((created is not None and begin <= created <= end) or
                      (first_seen is not None and first_seen <= end and
                       (last_seen is None or last_seen >= begin)))))
        if not in_trial:
            continue
        new_pod_uids.add(uid)
        new_pod_watch.append({**info, 'log_file_present': uid in files_by_uid,
                              'in_final_uid_set': uid in final_uids})
    trial_operator_uids = expected_uids | new_pod_uids
    missing_watch_uids = sorted(uid for uid in trial_operator_uids if uid not in pod_watch)
    no_manager_start_uids = sorted(
        uid for uid in trial_operator_uids
        if uid in pod_watch and not pod_watch[uid].get('manager_running_seen')
        and not pod_watch[uid].get('ever_ready'))
    log_or_pending_watch_gaps = sorted(
        uid for uid in trial_operator_uids
        if uid not in files_by_uid
        and uid not in no_manager_start_uids)
    old_coverage_uids = sorted(pre_uids)
    old_logs_present = bool(old_coverage_uids) and all(uid in files_by_uid for uid in old_coverage_uids)
    old_watch_present = bool(old_coverage_uids) and all(uid in pod_watch for uid in old_coverage_uids)
    old_pod_watch_rows = [
        {**pod_watch[uid], 'log_file_present': uid in files_by_uid}
        for uid in old_coverage_uids if uid in pod_watch]
    old_pod_still_ready = any(item.get('ready') is True and not item.get('deletion_timestamp')
                              and not item.get('deleted_seen') for item in old_pod_watch_rows)
    replacement_not_observed_ready = any(not item.get('ever_ready') for item in new_pod_watch)
    coverage_sufficient_for_censoring = (
        begin is not None and end is not None and bool(old_shutdowns)
        and old_logs_present and old_watch_present and not missing_watch_uids
        and not log_or_pending_watch_gaps)
    operational_handoffs = []
    for shutdown in old_shutdowns:
        following_starts = [m for m in starts_after if m['timestamp'] > shutdown['timestamp']]
        next_start = min(following_starts, key=lambda m: m['timestamp']) if following_starts else None
        following_finish = [m for m in markers if m['pod_uid'] == shutdown['pod_uid'] and
                            m['marker'] == 'all_workers_finished' and m['timestamp'] is not None and
                            m['timestamp'] > shutdown['timestamp'] and
                            (end is None or m['timestamp'] <= end)]
        own_finish = min(following_finish, key=lambda m: m['timestamp']) if following_finish else None
        operational_handoffs.append({
            'old_pod_uid': shutdown['pod_uid'],
            'shutdown_signal_utc': shutdown['timestamp_utc'],
            'old_all_workers_finished_utc': own_finish['timestamp_utc'] if own_finish else None,
            'old_drain_seconds': seconds_between(own_finish['timestamp'], shutdown['timestamp']) if own_finish else None,
            'next_start_pod_uid': next_start['pod_uid'] if next_start else None,
            'next_start_utc': next_start['timestamp_utc'] if next_start else None,
            'shutdown_signal_to_next_start_seconds': seconds_between(next_start['timestamp'], shutdown['timestamp']) if next_start else None,
            'shutdown_signal_to_next_start_lower_bound_seconds': (
                seconds_between(end, shutdown['timestamp'])
                if next_start is None and end is not None and
                   coverage_sufficient_for_censoring else None),
            'next_start_censoring_status': (
                'right_censored_by_trial_end_with_log_and_pod_coverage'
                if next_start is None and end is not None and
                   coverage_sufficient_for_censoring else
                ('not_quantified_incomplete_log_or_pod_coverage'
                 if next_start is None else 'observed')),
            'old_pod_log_present': shutdown['pod_uid'] in files_by_uid,
            'old_pod_watch_seen': shutdown['pod_uid'] in pod_watch,
            'missing_next_start_marker': next_start is None,
            'missing_old_finish_marker': own_finish is None,
        })

    # Union per-pod Starting-workers -> All-workers-finished intervals. Open intervals are
    # censored at trial end; intervals with no start are known active only up to their finish.
    clipped = []
    if begin is not None and end is not None:
        for interval in intervals:
            start, finish = interval['start'], interval['end']
            if finish is not None and finish < begin:
                continue
            if start is not None and start > end:
                continue
            clipped_start = max(start, begin) if start is not None else begin
            clipped_end = min(finish, end) if finish is not None else end
            if clipped_end < clipped_start:
                continue
            clipped.append({**interval, 'clipped_start': clipped_start, 'clipped_end': clipped_end,
                            'left_censored': start is None or start < begin,
                            'right_censored': finish is None or finish > end})
    clipped.sort(key=lambda interval: (interval['clipped_start'], interval['clipped_end']))
    merged = []
    overlap_transitions = []
    for current in clipped:
        if not merged or current['clipped_start'] > merged[-1]['end']:
            if merged:
                prior = merged[-1]
                gap_start, gap_end = prior['end'], current['clipped_start']
                if begin <= gap_start and gap_end <= end:
                    prior_marker = prior['union_end_marker']
                    next_marker = current['start_event']
                    merged[-1]['gaps_after'].append({
                        'start_utc': iso(gap_start), 'end_utc': iso(gap_end),
                        'seconds': seconds_between(gap_end, gap_start),
                        'after_pod_uid': prior_marker['pod_uid'],
                        'before_pod_uid': next_marker['pod_uid'] if next_marker else None,
                    })
            merged.append({'start': current['clipped_start'], 'end': current['clipped_end'],
                           'uids': [current['pod_uid']], 'intervals': [current],
                           'start_markers': [current['start_event']] if current['start_event'] else [],
                           'end_markers': [current['finish_event']] if current['finish_event'] else [],
                           'union_end_marker': current['finish_event'],
                           'gaps_after': []})
        else:
            prior = merged[-1]
            if current['clipped_start'] < prior['end']:
                overlap_transitions.append({
                    'earlier_or_active_pod_uids': sorted(set(prior['uids'])),
                    'new_pod_uid': current['pod_uid'],
                    'new_start_utc': iso(current['clipped_start']),
                    'prior_union_end_utc': iso(prior['end']),
                    'overlap_seconds': seconds_between(prior['end'], current['clipped_start']),
                    'note': 'worker-active intervals overlap, so this transition creates zero worker gap',
                })
            prior['end'] = max(prior['end'], current['clipped_end'])
            if current['clipped_end'] >= prior['end'] and current['finish_event'] is not None:
                prior['union_end_marker'] = current['finish_event']
            prior['uids'].append(current['pod_uid'])
            prior['intervals'].append(current)
            if current['start_event']:
                prior['start_markers'].append(current['start_event'])
            if current['finish_event']:
                prior['end_markers'].append(current['finish_event'])

    zero_gaps = []
    for union in merged:
        zero_gaps.extend(union['gaps_after'])
    relevant_intervals = clipped
    missing_stop_uids = sorted({shutdown['pod_uid'] for shutdown in old_shutdowns
                                if not any(m['pod_uid'] == shutdown['pod_uid'] and
                                           m['marker'] == 'all_workers_finished' and
                                           m['timestamp'] is not None and
                                           m['timestamp'] > shutdown['timestamp'] and
                                           (end is None or m['timestamp'] <= end)
                                           for m in markers)})
    missing_start_uids = sorted({item['pod_uid'] for item in relevant_intervals
                                 if item['start'] is None and item['end'] is not None})
    censored_zero_worker_tail = {
        'status': 'not_observed', 'start_utc': None, 'cutoff_utc': iso(end),
        'lower_bound_seconds': None,
        'definition': 'A right-censored interval after the last captured All workers finished marker, only when no later captured Starting workers interval covers the tail.',
    }
    if begin is None or end is None:
        censored_zero_worker_tail['status'] = 'unbounded_trial_window'
    elif zero_gaps:
        censored_zero_worker_tail['status'] = 'completed_gaps_observed'
    elif not old_shutdowns:
        censored_zero_worker_tail['status'] = 'no_old_shutdown_observed_no_gap_inferred'
    elif not starts_after:
        last_union = merged[-1] if merged else None
        last_end_marker = (last_union or {}).get('union_end_marker')
        last_end = (last_union or {}).get('end')
        if last_end_marker is None or last_end is None or last_end >= end:
            censored_zero_worker_tail['status'] = 'old_workers_not_proven_finished_before_trial_end'
        elif not coverage_sufficient_for_censoring:
            censored_zero_worker_tail.update({
                'status': 'censored_but_log_or_pod_coverage_insufficient',
                'start_utc': iso(last_end),
            })
        else:
            censored_zero_worker_tail.update({
                'status': 'right_censored_lower_bound_with_log_and_pod_coverage',
                'start_utc': iso(last_end),
                'lower_bound_seconds': seconds_between(end, last_end),
            })
    elif not coverage_sufficient_for_censoring:
        censored_zero_worker_tail['status'] = 'no_gap_claim_due_to_incomplete_log_or_pod_coverage'
    else:
        censored_zero_worker_tail['status'] = 'no_positive_gap_observed'
    censored_zero_worker_tail['coverage'] = {
        'sufficient_for_censored_lower_bound': coverage_sufficient_for_censoring,
        'old_operator_log_files_present': old_logs_present,
        'old_operator_pods_seen_in_watch': old_watch_present,
        'missing_watch_uids': missing_watch_uids,
        'uids_without_log_or_confirmed_never_started_pod': log_or_pending_watch_gaps,
    }
    if missing_uid_logs:
        errors.append('one or more pre/final operator UIDs have no discovered per-UID log file')
    if missing_stop_uids:
        errors.append('a shutdown marker inside the trial has no All workers finished marker before trial end')
    if missing_start_uids:
        errors.append('worker interval has All workers finished but no captured Starting workers marker')
    expected_worker_restart = 'delete-standby' not in str(trial_name or '')
    if expected_worker_restart and not old_shutdowns:
        errors.append('no old-pod shutdown-signal marker inside the trial window')
    if expected_worker_restart and not starts_after:
        errors.append('no Starting workers marker inside the trial window')

    return {
        'scenario': trial_name, 'trial': trial_no,
        'namespace_count': first_value(row, ['namespace_count']),
        'previous_count': first_value(row, ['previous_count', 'previous_namespace_count',
                                            'previous_healthy_namespace_count']),
        'desired_replicas': first_value(row, ['desired_replicas', 'replicas']),
        'event_file': row.get('event_file') or context['event_file'],
        'event_file_resolved': context['event_file'],
        'trigger_utc': iso(begin), 'trigger_time_source': context['trigger_source'],
        'finished_utc': iso(end), 'pre_operator_uids': sorted(pre_uids),
        'final_operator_uids': sorted(final_uids), 'missing_pre_or_final_uid_logs': missing_uid_logs,
        'cluster_controller_markers_in_window': [
            {'pod_uid': m['pod_uid'], 'pod_name': m['pod_name'], 'node': m['node'],
             'marker': m['marker'], 'timestamp_utc': m['timestamp_utc'],
             'controller': m['controller'], 'worker_count': m['worker_count']}
            for m in window_markers
        ],
        'shutdown_signal_to_next_start_handoffs': operational_handoffs,
        'operator_pod_watch_coverage': {
            'observed_pretrial_pods': old_pod_watch_rows,
            'observed_new_pods_during_trial': new_pod_watch,
            'missing_watch_uids': missing_watch_uids,
            'no_manager_start_or_ready_seen_uids': no_manager_start_uids,
        },
        'worker_gap_interpretation': {
            'strict_zero_worker_gap_status': censored_zero_worker_tail['status'],
            'censored_zero_worker_tail': censored_zero_worker_tail,
            'old_controller_may_have_remained_active_while_replacement_not_ready': (
                not old_shutdowns and old_pod_still_ready and replacement_not_observed_ready),
            'old_controller_scope_caution': (
                'An old operator Pod was still Ready at its last captured watch event while no replacement Pod was observed Ready. A missing shutdown/start pair here is not evidence that reconciliation stopped; the old Pod may still be reconciling its prior WATCH_NAMESPACE scope.'
                if not old_shutdowns and old_pod_still_ready and replacement_not_observed_ready else None),
            'no_shutdown_caution': (
                'No old Cluster-worker shutdown marker was captured. Do not interpret an empty gap list as a measured zero: an old operator Pod may have remained active while its replacement was pending or not Ready.'
                if not old_shutdowns else None),
        },
        'worker_restart_expected': expected_worker_restart,
        'worker_active_interval_union': {
            'definition': 'Union of each Cluster controller per-Pod Starting workers to All workers finished interval.',
            'zero_worker_gaps_wholly_inside_trial': zero_gaps,
            'overlapping_transitions': overlap_transitions,
            'intervals_intersecting_trial': [
                {'pod_uid': item['pod_uid'],
                 'start_utc': iso(item['start']), 'all_workers_finished_utc': iso(item['end']),
                 'clipped_start_utc': iso(item['clipped_start']), 'clipped_end_utc': iso(item['clipped_end']),
                 'left_censored_at_trial_start': item['left_censored'],
                 'right_censored_at_trial_end': item['right_censored']}
                for item in relevant_intervals
            ],
            'missing_start_marker_uids': missing_start_uids,
            'missing_all_workers_finished_marker_uids': missing_stop_uids,
        },
        'marker_coverage': {
            'pre_uid_logs_present': not any(uid not in files_by_uid for uid in pre_uids),
            'final_uid_logs_present': not any(uid not in files_by_uid for uid in final_uids),
            'old_shutdown_signal_seen': bool(old_shutdowns),
            'starting_workers_seen': bool(starts_after),
            'all_workers_finished_seen': bool(finished),
            'complete': not errors,
            'limitations': errors,
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory', help='AKS run directory containing benchmark/results.json and operator log collector output')
    args = parser.parse_args()
    run_dir = Path(args.run_directory).resolve()
    results_path, rows = find_results(run_dir)
    contexts = [trial_context(row, results_path, run_dir) for row in rows]
    markers, files_by_uid, duplicates, unknown, discovered_log_files, pod_watch = load_logs(run_dir)
    intervals = build_intervals(markers)
    trials = [analyze_trial(context, markers, intervals, files_by_uid, pod_watch)
              for context in contexts]
    output = {
        'artifact_type': 'cnpg_cluster_controller_worker_gap_analysis',
        'source': {'run_directory': str(run_dir), 'benchmark_results': str(results_path),
                   'per_uid_log_file_count': discovered_log_files},
        'method': {
            'controller_filter': 'Only structured log entries whose controller is cluster or whose logger/name ends in .cluster are included.',
            'deduplication': 'Repeated log retrieval is deduplicated by (Pod UID, timestamp, message).',
            'strict_zero_worker_gap': 'Build per-Pod intervals from Starting workers to All workers finished, union intervals across Pod UIDs, and report only uncovered gaps wholly inside trigger_utc..finished_utc. Overlapping intervals remain covered and are also reported as overlap transitions.',
            'drain_aware_gap': 'For each old-Pod Shutdown signal received marker in the trial, report time to its All workers finished marker and to the next Cluster Starting workers marker.',
            'censoring': 'An empty completed-gap list is not reported as a zero-duration outage. If old workers finish and no later start is captured, a right-censored lower bound is emitted only when old Pod logs and Pod-watch coverage support it. If no old shutdown marker is captured, no worker-gap value is inferred; the old controller may still be serving while a replacement is Pending or not Ready.',
            'clock_skew': 'kubectl logs --timestamps supplies container/node-side timestamps. Comparing Pods on different AKS nodes can include node clock skew; benchmark UTC, Pod watch receipt times, and condition transitions are separate clocks/evidence. Treat subsecond cross-node differences as approximate unless node clock offset is measured.',
            'marker_semantics': 'Shutdown signal received is emitted after the controller context is canceled and before the worker wait. All workers finished follows wg.Wait. It does not prove every reconcile had completed successfully; cancellations and logs must be reviewed alongside the interval.',
        },
        'log_parse': {'cluster_controller_marker_count': len(markers),
                      'duplicate_lines_discarded': duplicates,
                      'marker_candidates_without_identifiable_controller': unknown,
                      'worker_intervals': len(intervals)},
        'trial_count': len(trials), 'trials': trials,
    }
    output_path = run_dir / 'worker-gaps.json'
    output_path.write_text(json.dumps(output, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    print(output_path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
