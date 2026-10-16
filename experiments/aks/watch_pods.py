#!/usr/bin/env python3
"""Stream sanitized Pod watch events with local receive timestamps."""
import argparse
import codecs
import datetime
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True, help='explicit kubectl context')
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--selector', required=True, help='Pod label selector')
    parser.add_argument('--output', required=True, help='output JSONL file')
    return parser.parse_args()


def timestamp_utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def pod_event_row(watch_event):
    pod = watch_event.get('object') or {}
    metadata = pod.get('metadata') or {}
    spec = pod.get('spec') or {}
    status = pod.get('status') or {}
    conditions = status.get('conditions') or []
    return {
        'type': watch_event.get('type'),
        'received_utc': timestamp_utc(),
        'received_monotonic': time.monotonic(),
        'pod_name': metadata.get('name'),
        'pod_uid': metadata.get('uid'),
        'node': spec.get('nodeName'),
        'ready': any(condition.get('type') == 'Ready' and condition.get('status') == 'True'
                     for condition in conditions),
        'condition_transitions': [
            {'type': condition.get('type'),
             'status': condition.get('status'),
             'last_transition_time': condition.get('lastTransitionTime')}
            for condition in conditions
        ],
        'containers': {
            container.get('name'): {
                'running_started_at': (container.get('state') or {}).get('running', {}).get('startedAt'),
                'restart_count': container.get('restartCount', 0),
            }
            for container in status.get('containerStatuses', [])
        },
        'action_labels': {
            key: value for key, value in (metadata.get('labels') or {}).items()
            if key == 'restart-lab.example/action' or key.startswith('restart-lab.example/action-')
        },
    }


def decoded_frames(buffer, decoder):
    """Yield complete JSON objects from kubectl's streamed output."""
    frames = []
    while True:
        offset = len(buffer) - len(buffer.lstrip())
        if offset == len(buffer):
            return frames, ''
        try:
            frame, end = decoder.raw_decode(buffer, offset)
        except json.JSONDecodeError:
            return frames, buffer
        frames.append(frame)
        buffer = buffer[end:]


def main():
    args = arguments()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    command = [
        'kubectl', '--context', args.context, '--request-timeout=0',
        '-n', args.namespace, 'get', 'pods', '-l', args.selector,
        '--watch', '--output-watch-events', '-o', 'json',
    ]
    process = None
    stopping = False

    def stop_child(_signum, _frame):
        nonlocal stopping
        stopping = True
        if process is not None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    old_sigterm = signal.signal(signal.SIGTERM, stop_child)
    old_sigint = signal.signal(signal.SIGINT, stop_child)
    decoder = json.JSONDecoder()
    utf8_decoder = codecs.getincrementaldecoder('utf-8')()
    buffer = ''
    exit_code = 0
    try:
        if not stopping:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                       bufsize=0, start_new_session=True)
        if process is None:
            return 0
        with output.open('w', encoding='utf-8', buffering=1) as event_file:
            while not stopping:
                chunk = os.read(process.stdout.fileno(), 65536)
                if not chunk:
                    buffer += utf8_decoder.decode(b'', final=True)
                    break
                buffer += utf8_decoder.decode(chunk)
                frames, buffer = decoded_frames(buffer, decoder)
                for frame in frames:
                    if not isinstance(frame, dict):
                        continue
                    event_file.write(json.dumps(pod_event_row(frame), separators=(',', ':')) + '\n')
                    event_file.flush()
            # A final complete frame can arrive with EOF without a trailing newline.
            if buffer.strip():
                try:
                    frame = json.loads(buffer)
                    if isinstance(frame, dict):
                        event_file.write(json.dumps(pod_event_row(frame), separators=(',', ':')) + '\n')
                        event_file.flush()
                except json.JSONDecodeError:
                    pass
    finally:
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
        if process is not None:
            exit_code = process.returncode or 0
        signal.signal(signal.SIGTERM, old_sigterm)
        signal.signal(signal.SIGINT, old_sigint)
    if stopping:
        return 0
    if exit_code:
        print(f'kubectl watch exited with status {exit_code}', file=sys.stderr)
        return exit_code
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
