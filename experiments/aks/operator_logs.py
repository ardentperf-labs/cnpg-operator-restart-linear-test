#!/usr/bin/env python3
"""Collect CNPG operator pod lifecycle events and per-UID manager logs."""
import argparse
import codecs
import datetime
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time

from watch_pods import decoded_frames


DEFAULT_SELECTOR = 'app.kubernetes.io/name=cloudnative-pg'
CONTAINER = 'manager'


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True, help='explicit kubectl context')
    parser.add_argument('--namespace', default='cnpg-system')
    parser.add_argument('--selector', default=DEFAULT_SELECTOR)
    parser.add_argument('--output', required=True, help='output directory')
    return parser.parse_args()


def timestamp_utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def pod_event_row(watch_event):
    pod = watch_event.get('object') or {}
    metadata = pod.get('metadata') or {}
    spec = pod.get('spec') or {}
    status = pod.get('status') or {}
    conditions = status.get('conditions') or []
    containers = {}
    for container in status.get('containerStatuses') or []:
        state = container.get('state') or {}
        containers[container.get('name')] = {
            'running_started_at': (state.get('running') or {}).get('startedAt'),
            'terminated_finished_at': (state.get('terminated') or {}).get('finishedAt'),
            'terminated_exit_code': (state.get('terminated') or {}).get('exitCode'),
            'restart_count': container.get('restartCount', 0),
        }
    return {
        'type': watch_event.get('type'),
        'received_utc': timestamp_utc(),
        'received_monotonic': time.monotonic(),
        'pod_name': metadata.get('name'),
        'pod_uid': metadata.get('uid'),
        'node': spec.get('nodeName'),
        'creation_timestamp': metadata.get('creationTimestamp'),
        'deletion_timestamp': metadata.get('deletionTimestamp'),
        'phase': status.get('phase'),
        'ready': any(condition.get('type') == 'Ready' and condition.get('status') == 'True'
                     for condition in conditions),
        'condition_transitions': [
            {'type': condition.get('type'),
             'status': condition.get('status'),
             'last_transition_time': condition.get('lastTransitionTime')}
            for condition in conditions
        ],
        'containers': containers,
    }


def record_event(file, event, **fields):
    row = {'event': event, 'utc': timestamp_utc(), 'monotonic': time.monotonic()}
    row.update(fields)
    file.write(json.dumps(row, separators=(',', ':')) + '\n')
    file.flush()


def terminate_group(process, sig=signal.SIGTERM):
    if process is None or process.poll() is not None:
        return
    try:
        os.killpg(process.pid, sig)
    except ProcessLookupError:
        pass


def reap_process(process, timeout=3):
    if process is None:
        return
    if process.poll() is None:
        terminate_group(process)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            terminate_group(process, signal.SIGKILL)
            process.wait()
    else:
        process.wait()


def main():
    args = arguments()
    out = Path(args.output)
    logs_dir = out / 'logs'
    errors_dir = out / 'stderr'
    logs_dir.mkdir(parents=True, exist_ok=True)
    errors_dir.mkdir(parents=True, exist_ok=True)

    watch_command = [
        'kubectl', '--context', args.context, '--request-timeout=0',
        '-n', args.namespace, 'get', 'pods', '-l', args.selector,
        '--watch', '--output-watch-events', '-o', 'json',
    ]
    pods = {}
    watch_process = None
    stopping = False
    watch_attempt = 0
    next_watch_start = 0.0
    selector = selectors.DefaultSelector()
    utf8_decoder = codecs.getincrementaldecoder('utf-8')()
    buffer = ''

    events_path = out / 'operator-watch.jsonl'
    follower_events_path = out / 'log-followers.jsonl'

    def stop_children(_signum, _frame):
        nonlocal stopping
        stopping = True
        terminate_group(watch_process)
        for pod_state in pods.values():
            terminate_group(pod_state.get('process'))

    previous_sigterm = signal.signal(signal.SIGTERM, stop_children)
    previous_sigint = signal.signal(signal.SIGINT, stop_children)

    def spawn_follower(pod_state, follower_file):
        process = pod_state.get('process')
        if stopping or pod_state.get('deleted') or (process and process.poll() is None):
            return
        pod_state['attempt'] = pod_state.get('attempt', 0) + 1
        attempt = pod_state['attempt']
        name = pod_state['name']
        uid = pod_state['uid']
        log_path = logs_dir / f'{name}-{uid}.log'
        stderr_path = errors_dir / f'{uid}-attempt-{attempt:02d}.stderr'
        command = [
            'kubectl', '--context', args.context, '--request-timeout=0',
            '-n', args.namespace, 'logs', name,
            '--follow', '--pod-running-timeout=60s',
            '-c', CONTAINER, '--timestamps',
        ]
        with log_path.open('ab') as stdout_file, stderr_path.open('ab') as stderr_file:
            process = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file,
                                       start_new_session=True, bufsize=0)
        pod_state['process'] = process
        pod_state['next_retry'] = None
        pod_state['log_path'] = str(log_path)
        pod_state['exit_recorded'] = False
        record_event(follower_file, 'log_follow_started', pod_name=name, pod_uid=uid,
                     attempt=attempt, pid=process.pid, log_file=str(log_path),
                     stderr_file=str(stderr_path))

    def service_followers(follower_file):
        now = time.monotonic()
        for state in pods.values():
            process = state.get('process')
            if process is not None:
                exit_code = process.poll()
                if exit_code is not None:
                    state['process'] = None
                    if not state.get('exit_recorded'):
                        record_event(follower_file, 'log_follow_exited', pod_name=state['name'],
                                     pod_uid=state['uid'], attempt=state.get('attempt'),
                                     exit_code=exit_code, log_file=state.get('log_path'))
                        state['exit_recorded'] = True
                    if not state.get('deleted'):
                        delay = min(8.0, 0.5 * (2 ** min(state.get('attempt', 1) - 1, 4)))
                        state['next_retry'] = now + delay
            if (not stopping and not state.get('deleted') and state.get('process') is None
                    and state.get('next_retry') is not None and now >= state['next_retry']):
                spawn_follower(state, follower_file)

    def handle_watch_event(watch_event, event_file, follower_file):
        row = pod_event_row(watch_event)
        event_file.write(json.dumps(row, separators=(',', ':')) + '\n')
        event_file.flush()
        uid = row.get('pod_uid')
        name = row.get('pod_name')
        if not uid or not name:
            return
        state = pods.get(uid)
        if state is None:
            state = {'uid': uid, 'name': name, 'attempt': 0, 'process': None,
                     'deleted': False, 'next_retry': None}
            pods[uid] = state
            record_event(follower_file, 'pod_seen', pod_name=name, pod_uid=uid,
                         watch_type=row.get('type'), node=row.get('node'))
        else:
            state['name'] = name
        if row.get('type') == 'DELETED':
            state['deleted'] = True
            record_event(follower_file, 'pod_deleted', pod_name=name, pod_uid=uid)
            return
        state['deleted'] = False
        if state.get('process') is None and state.get('next_retry') is None:
            spawn_follower(state, follower_file)

    def start_watch(event_file):
        nonlocal watch_process, watch_attempt, utf8_decoder, buffer
        watch_attempt += 1
        stderr_path = errors_dir / f'watch-attempt-{watch_attempt:02d}.stderr'
        stderr_file = stderr_path.open('ab')
        watch_process = subprocess.Popen(watch_command, stdout=subprocess.PIPE,
                                         stderr=stderr_file, bufsize=0,
                                         start_new_session=True)
        stderr_file.close()
        selector.register(watch_process.stdout, selectors.EVENT_READ)
        utf8_decoder = codecs.getincrementaldecoder('utf-8')()
        buffer = ''
        record_event(event_file, 'pod_watch_started', attempt=watch_attempt,
                     pid=watch_process.pid, stderr_file=str(stderr_path),
                     context=args.context, namespace=args.namespace, selector=args.selector)

    exit_code = 0
    try:
        with events_path.open('a', encoding='utf-8', buffering=1) as event_file, \
             follower_events_path.open('a', encoding='utf-8', buffering=1) as follower_file:
            while not stopping:
                now = time.monotonic()
                if watch_process is None and now >= next_watch_start:
                    start_watch(event_file)
                service_followers(follower_file)
                ready = selector.select(timeout=0.5) if watch_process is not None else []
                for key, _ in ready:
                    try:
                        chunk = os.read(key.fileobj.fileno(), 65536)
                    except OSError:
                        chunk = b''
                    if chunk:
                        buffer += utf8_decoder.decode(chunk)
                        frames, buffer = decoded_frames(buffer, json.JSONDecoder())
                        for frame in frames:
                            if isinstance(frame, dict):
                                handle_watch_event(frame, event_file, follower_file)
                    else:
                        try:
                            buffer += utf8_decoder.decode(b'', final=True)
                        except UnicodeDecodeError:
                            pass
                        frames, buffer = decoded_frames(buffer, json.JSONDecoder())
                        for frame in frames:
                            if isinstance(frame, dict):
                                handle_watch_event(frame, event_file, follower_file)
                        if buffer.strip():
                            try:
                                frame = json.loads(buffer)
                                if isinstance(frame, dict):
                                    handle_watch_event(frame, event_file, follower_file)
                            except json.JSONDecodeError:
                                record_event(event_file, 'incomplete_watch_frame', bytes=len(buffer))
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                        old_process = watch_process
                        watch_process = None
                        if old_process is not None:
                            old_process.wait()
                            record_event(event_file, 'pod_watch_exited', attempt=watch_attempt,
                                         exit_code=old_process.returncode)
                        next_watch_start = time.monotonic() + 1.0
                if watch_process is not None and watch_process.poll() is not None:
                    # EOF is normally selected first, but guard against unusual pipe behavior.
                    try:
                        selector.unregister(watch_process.stdout)
                    except (KeyError, ValueError):
                        pass
                    watch_process.stdout.close()
                    old_process = watch_process
                    watch_process = None
                    old_process.wait()
                    record_event(event_file, 'pod_watch_exited', attempt=watch_attempt,
                                 exit_code=old_process.returncode)
                    next_watch_start = time.monotonic() + 1.0
            exit_code = 0
    except FileNotFoundError as error:
        print(f'required executable unavailable: {error}', file=sys.stderr)
        exit_code = 127
    finally:
        selector.close()
        reap_process(watch_process)
        for state in pods.values():
            reap_process(state.get('process'))
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.signal(signal.SIGINT, previous_sigint)
    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())
