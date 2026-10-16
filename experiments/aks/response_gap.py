#!/usr/bin/env python3
"""Measure continuous CNPG action-response gaps during operator disruptions.

This runs against an already prepared disposable cluster. It does not provision
cloud resources. Each probe is a unique inherited Cluster label update. The
next update is not sent until all three ready database Pods show the preceding
value, so reconciliation cannot skip an update because a later value replaced
it. The measurement is observed Pod-label propagation, not a direct measure of
whether a controller process was running at every instant.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout
import datetime
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


LABEL = 'restart-lab.example/action'
DB_NAMESPACE = 'cnpg-lab-0000'
DB_CLUSTER = 'probe-000'
OPERATOR_NAMESPACE = 'cnpg-system'
DEPLOYMENT = 'cnpg-controller-manager'
LEASE = 'db9c8771.cnpg.io'
EXPECTED_DB_PODS = 3
SCALE_COUNTS_DEFAULT = [3, 30, 100, 300, 1000, 2000, 4000, 8000, 9999]
SCALE_UNSTABLE_WORKER_TIMEOUT = 120
SCALE_HEALTH_POLL_SECONDS = 2


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True, help='explicit kubectl context')
    parser.add_argument('--output', required=True, help='directory for trial JSONL and summary JSON')
    parser.add_argument('--mode', choices=('benchmark', 'scale-until-unstable'), default='benchmark',
                        help='fixed namespace-count cases or increasing scale screen')
    parser.add_argument('--repeat', type=int, default=None,
                        help='repetitions per case (default: 3 benchmark, 2 scale mode)')
    parser.add_argument('--namespace-counts', default='3,30,100',
                        help='WATCH_NAMESPACE sizes to measure (default: 3,30,100)')
    parser.add_argument('--scale-counts', default=','.join(map(str, SCALE_COUNTS_DEFAULT)),
                        help=('increasing WATCH_NAMESPACE sizes for scale mode, starting at 3 or higher '
                              '(default: 3,30,100,300,1000,2000,4000,8000,9999)'))
    parser.add_argument('--scale-replicas', default='1,2',
                        help='replica counts measured at each namespace level in this order (default: 1,2)')
    parser.add_argument('--scale-leader-delete', action='store_true',
                        help='also run graceful leader-delete probes at each scale point')
    parser.add_argument('--disposable', action='store_true',
                        help='skip Deployment/scope restoration and namespace deletion on exit')
    parser.add_argument('--baseline-seconds', type=float, default=5,
                        help='continuous probes before each disruption')
    parser.add_argument('--post-seconds', type=float, default=20,
                        help='minimum observation window after recovery (default: 20s)')
    parser.add_argument('--post-actions', type=int, default=3,
                        help='distinct completed probes required after recovery (default: 3)')
    parser.add_argument('--probe-interval', type=float, default=0.25,
                        help='minimum delay after a completed action before the next probe')
    parser.add_argument('--action-timeout', type=float, default=120,
                        help='maximum wait for one accepted action before stopping that stream')
    parser.add_argument('--recovery-timeout', type=float, default=180,
                        help='maximum wait for an operator restart or Pod replacement')
    parser.add_argument('--deadline-seconds', type=float, default=None,
                        help=('whole-run time limit including cleanup (default: 900s benchmark, '
                              '1800s scale mode; increase for a longer scale sequence)'))
    args = parser.parse_args()
    if args.repeat is None:
        args.repeat = 2 if args.mode == 'scale-until-unstable' else 3
    if args.deadline_seconds is None:
        args.deadline_seconds = 1800 if args.mode == 'scale-until-unstable' else 900
    try:
        args.namespace_counts = [int(value.strip()) for value in args.namespace_counts.split(',')]
        args.scale_counts = [int(value.strip()) for value in args.scale_counts.split(',')]
        args.scale_replicas = [int(value.strip()) for value in args.scale_replicas.split(',')]
    except ValueError:
        parser.error('--namespace-counts, --scale-counts, and --scale-replicas must be comma-separated integers')
    if (not args.namespace_counts or min(args.namespace_counts) < 1
            or max(args.namespace_counts) > 9999):
        parser.error('--namespace-counts values must be in the range 1..9999')
    if (not args.scale_counts or any(n < 3 or n > 9999 for n in args.scale_counts)
            or any(right <= left for left, right in zip(args.scale_counts, args.scale_counts[1:]))):
        parser.error('--scale-counts must be strictly increasing and stay in 3..9999')
    if (not args.scale_replicas or any(r not in (1, 2) for r in args.scale_replicas)
            or len(set(args.scale_replicas)) != len(args.scale_replicas)):
        parser.error('--scale-replicas must contain unique values from 1,2')
    if args.repeat < 1:
        parser.error('--repeat must be positive')
    if args.post_actions < 1:
        parser.error('--post-actions must be positive')
    if min(args.baseline_seconds, args.post_seconds, args.probe_interval,
           args.action_timeout, args.recovery_timeout, args.deadline_seconds) <= 0:
        parser.error('all duration options must be positive')
    return args


ARGS = parse_args()
KUBECTL = ['kubectl', '--context', ARGS.context, '--request-timeout=10s']
OUT = Path(ARGS.output)
OUT.mkdir(parents=True, exist_ok=True)
RUN_ID = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8]
RUN_STARTED = time.monotonic()
RUN_DEADLINE = RUN_STARTED + ARGS.deadline_seconds
RUN_DEADLINE_NS = int(RUN_DEADLINE * 1e9)
ORIGINAL_REPLICAS = None
ORIGINAL_WATCH_NAMESPACE = None
CREATED_NAMESPACES = []
KNOWN_NAMESPACES = None
ACTIVE_LOGGER = None
STOPPING = threading.Event()
PROXY = None
PROXY_LOG = None
OPERATOR_LOG_PROCESS = None
OPERATOR_LOG_STDOUT = None
SUMMARY_LOCK = threading.RLock()


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def kubectl(args, input_text=None, timeout=20, check=True):
    result = subprocess.run(KUBECTL + args, input=input_text, capture_output=True,
                            text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f'kubectl {" ".join(args[:4])} exited {result.returncode}: '
                           f'{result.stderr.strip() or "no error text"}')
    return result


def kube_json(args):
    return json.loads(kubectl(args + ['-o', 'json']).stdout)


class EventLog:
    def __init__(self, path):
        self.path = Path(path)
        self.file = self.path.open('w', encoding='utf-8', buffering=1)
        self.lock = threading.Lock()

    def emit(self, event, **fields):
        row = {'event': event, 'utc': utc_now(), 'monotonic_ns': time.monotonic_ns(), **fields}
        with self.lock:
            self.file.write(json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n')
            self.file.flush()
        return row

    def close(self):
        with self.lock:
            if not self.file.closed:
                self.file.flush()
                self.file.close()


def deployment():
    return kube_json(['-n', OPERATOR_NAMESPACE, 'get', 'deployment', DEPLOYMENT])


def deployment_selector(deploy):
    labels = deploy.get('spec', {}).get('selector', {}).get('matchLabels', {})
    if not labels:
        raise RuntimeError('operator Deployment has no matchLabels selector')
    return ','.join(f'{key}={value}' for key, value in sorted(labels.items()))


def operator_pods(deploy=None):
    deploy = deploy or deployment()
    return kube_json(['-n', OPERATOR_NAMESPACE, 'get', 'pods', '-l',
                      deployment_selector(deploy)])['items']


def ready(pod):
    return any(item.get('type') == 'Ready' and item.get('status') == 'True'
               for item in pod.get('status', {}).get('conditions', []))


def pod_identity(pod):
    metadata = pod.get('metadata', {})
    statuses = pod.get('status', {}).get('containerStatuses', [])
    container_states = {}
    for item in statuses:
        state = item.get('state') or {}
        waiting = state.get('waiting') or {}
        terminated = state.get('terminated') or {}
        last_terminated = (item.get('lastState') or {}).get('terminated') or {}
        waiting_message = ' '.join((waiting.get('message') or '',
                                    terminated.get('message') or '',
                                    last_terminated.get('message') or '')).lower()
        runtime_error = ('environment_size_limit'
                         if any(token in waiting_message for token in
                                ('argument list too long', 'e2big', 'environment variable too large',
                                 'environment list too large', 'too many arguments'))
                         else None)
        container_states[item.get('name')] = {
            'waiting_reason': waiting.get('reason'),
            'last_terminated_reason': last_terminated.get('reason'),
            'runtime_error_class': runtime_error,
            'restart_count': item.get('restartCount', 0),
        }
    return {'name': metadata.get('name'), 'uid': metadata.get('uid'),
            'resource_version': metadata.get('resourceVersion'), 'ready': ready(pod),
            'deleting': bool(metadata.get('deletionTimestamp')),
            'container_restarts': {item.get('name'): item.get('restartCount', 0)
                                   for item in statuses},
            'container_states': container_states}


def env_value(deploy, name):
    for container in deploy.get('spec', {}).get('template', {}).get('spec', {}).get('containers', []):
        if container.get('name') == 'manager':
            for item in container.get('env', []):
                if item.get('name') == name:
                    if 'value' in item:
                        return item.get('value')
                    raise RuntimeError(f'{name} uses valueFrom; refusing to replace it')
    return None


def set_watch_namespaces(value):
    deploy = deployment()
    current = env_value(deploy, 'WATCH_NAMESPACE')
    if current == value:
        return False
    kubectl(['-n', OPERATOR_NAMESPACE, 'set', 'env', f'deployment/{DEPLOYMENT}',
             'WATCH_NAMESPACE=' + value])
    return True


def wait_deployment_ready(replicas, old_uids=None, timeout=None, require_new=False):
    deadline = time.monotonic() + (timeout or ARGS.recovery_timeout)
    last = None
    while time.monotonic() < deadline and time.monotonic() < RUN_DEADLINE:
        deploy = deployment()
        pods = operator_pods(deploy)
        meta = deploy.get('metadata', {})
        spec = deploy.get('spec', {})
        status = deploy.get('status', {})
        active = [pod for pod in pods if not pod.get('metadata', {}).get('deletionTimestamp')]
        all_ready = len(active) == replicas and all(ready(pod) for pod in active)
        new_ready = (not require_new or all(pod.get('metadata', {}).get('uid') not in (old_uids or set())
                                            for pod in active))
        okay = (spec.get('replicas', 1) == replicas
                and (status.get('observedGeneration') or 0) >= (meta.get('generation') or 0)
                and status.get('updatedReplicas', 0) == replicas
                and status.get('availableReplicas', 0) == replicas
                and status.get('unavailableReplicas', 0) == 0
                and all_ready and new_ready)
        last = {'generation': meta.get('generation'), 'observed_generation': status.get('observedGeneration'),
                'desired_replicas': spec.get('replicas'), 'updated_replicas': status.get('updatedReplicas'),
                'available_replicas': status.get('availableReplicas'),
                'pods': [pod_identity(pod) for pod in pods]}
        if okay:
            return deploy, pods
        time.sleep(0.4)
    raise TimeoutError(f'Deployment did not reach {replicas} ready replicas: {last}')


def create_namespace_via_proxy(base, name):
    url = base + '/api/v1/namespaces'
    payload = json.dumps({'apiVersion': 'v1', 'kind': 'Namespace',
                          'metadata': {'name': name}}).encode()
    for attempt in range(4):
        request = urllib.request.Request(url, data=payload, method='POST', headers={
            'Content-Type': 'application/json', 'Accept': 'application/json'})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                response.read()
                return name, response.status == 201
        except urllib.error.HTTPError as error:
            detail = error.read().decode('utf-8', errors='replace')[:500]
            if error.code == 409:
                return name, False
            if error.code not in (429, 500, 502, 503, 504) or attempt == 3:
                raise RuntimeError(f'Namespace create {name} returned HTTP {error.code}: {detail}')
        if attempt < 3:
            time.sleep(0.2 * (attempt + 1))
    raise RuntimeError(f'Namespace create {name} exhausted retries')


def ensure_namespaces(count, base=None):
    global KNOWN_NAMESPACES
    names = [f'cnpg-lab-{i:04d}' for i in range(count)]
    if base is not None:
        if KNOWN_NAMESPACES is None:
            listed = api_json(base, '/api/v1/namespaces', timeout=30)
            KNOWN_NAMESPACES = {item['metadata']['name']
                                for item in listed.get('items', [])}
        missing = [name for name in names if name not in KNOWN_NAMESPACES]
        if not missing:
            return names
        deadline = min(RUN_DEADLINE, time.monotonic() + 180)
        if time.monotonic() >= deadline:
            raise TimeoutError('namespace creation has no remaining run time')
        pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix='namespace-create')
        wait_for_threads = True
        completed = 0
        errors = []
        try:
            futures = [pool.submit(create_namespace_via_proxy, base, name)
                       for name in missing]
            try:
                for future in as_completed(futures, timeout=max(0.1, deadline - time.monotonic())):
                    try:
                        name, created = future.result()
                        KNOWN_NAMESPACES.add(name)
                        if created:
                            CREATED_NAMESPACES.append(name)
                        completed += 1
                    except Exception as error:
                        errors.append(f'{type(error).__name__}: {error}')
            except FuturesTimeout:
                wait_for_threads = False
                raise TimeoutError(f'namespace creation exceeded its 180s/run-deadline bound '
                                   f'({completed}/{len(missing)} responses received)')
        finally:
            pool.shutdown(wait=wait_for_threads, cancel_futures=not wait_for_threads)
        if errors:
            raise RuntimeError(f'namespace creation failed for {len(errors)} namespace(s); '
                               f'completed {completed}/{len(missing)}: {errors[0]}')
        return names

    existing = set(item['metadata']['name'] for item in
                   kube_json(['get', 'namespaces']).get('items', []))
    missing = [name for name in names if name not in existing]
    # Keep requests bounded while a scale run adds namespaces level by level.
    for offset in range(0, len(missing), 200):
        batch = missing[offset:offset + 200]
        manifest = {'apiVersion': 'v1', 'kind': 'List', 'items': [
            {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': name}}
            for name in batch]}
        kubectl(['apply', '-f', '-'], input_text=json.dumps(manifest), timeout=60)
        CREATED_NAMESPACES.extend(batch)
    return names


def proxy_base_url():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    return f'http://127.0.0.1:{port}'


def start_proxy():
    global PROXY, PROXY_LOG
    base = proxy_base_url()
    port = int(base.rsplit(':', 1)[1])
    PROXY_LOG = (OUT / 'kubectl-proxy.log').open('ab')
    PROXY = subprocess.Popen(
        ['kubectl', '--context', ARGS.context, 'proxy', '--address=127.0.0.1',
         f'--port={port}', '--accept-hosts=^127\\.0\\.0\\.1$'],
        stdout=PROXY_LOG, stderr=PROXY_LOG, start_new_session=True)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if PROXY.poll() is not None:
            raise RuntimeError(f'kubectl proxy exited early with status {PROXY.returncode}')
        try:
            with urllib.request.urlopen(base + '/version', timeout=1) as response:
                if response.status == 200:
                    return base
        except Exception:
            time.sleep(0.1)
    raise TimeoutError('kubectl proxy did not start')


def api_json(base, path, params=None, timeout=10):
    query = '?' + urllib.parse.urlencode(params) if params else ''
    with urllib.request.urlopen(base + path + query, timeout=timeout) as response:
        return json.loads(response.read())


def patch_watch_namespaces(base, value):
    """Patch WATCH_NAMESPACE via the local proxy, avoiding argv-size limits."""
    path = f'/apis/apps/v1/namespaces/{OPERATOR_NAMESPACE}/deployments/{DEPLOYMENT}'
    patch = {'spec': {'template': {'spec': {'containers': [
        {'name': 'manager', 'env': [{'name': 'WATCH_NAMESPACE', 'value': value}]}
    ]}}}}
    payload = json.dumps(patch).encode()
    request = urllib.request.Request(base + path, data=payload, method='PATCH', headers={
        'Content-Type': 'application/strategic-merge-patch+json',
        'Accept': 'application/json'})
    started = time.monotonic_ns()
    with urllib.request.urlopen(request, timeout=15) as response:
        result = json.loads(response.read())
        finished = time.monotonic_ns()
    return {'changed': True, 'started_monotonic_ns': started,
            'finished_monotonic_ns': finished,
            'request_seconds': (finished - started) / 1e9,
            'namespace_value_characters': len(value),
            'namespace_value_utf8_bytes': len(value.encode()),
            'generation': result.get('metadata', {}).get('generation'),
            'resource_version': result.get('metadata', {}).get('resourceVersion')}


def cluster_worker_start_markers(uids, after_utc=None, offsets=None):
    """Find new Cluster-controller worker-start log records for selected UIDs."""
    logs_dir = OUT / 'operator-logs' / 'logs'
    if not logs_dir.exists():
        return []
    after = datetime.datetime.fromisoformat(after_utc) if after_utc else None
    found = []
    for uid in uids:
        for path in logs_dir.glob(f'*-{uid}.log'):
            try:
                with path.open('rb') as stream:
                    stream.seek((offsets or {}).get(uid, 0))
                    for raw in stream:
                        line = raw.decode('utf-8', errors='replace').strip()
                        prefix, separator, payload = line.partition(' ')
                        if not separator:
                            continue
                        try:
                            stamp = datetime.datetime.fromisoformat(
                                prefix.replace('Z', '+00:00'))
                            entry = json.loads(payload)
                        except (ValueError, json.JSONDecodeError):
                            continue
                        if (entry.get('msg') != 'Starting workers'
                                or entry.get('controller') != 'cluster'):
                            continue
                        if after is not None and stamp < after - datetime.timedelta(seconds=2):
                            continue
                        found.append({'pod_uid': uid, 'pod_log': path.name,
                                      'timestamp_utc': stamp.isoformat(),
                                      'worker_count': entry.get('worker count')})
            except OSError:
                continue
    return found


def operator_log_offsets(uids):
    logs_dir = OUT / 'operator-logs' / 'logs'
    offsets = {}
    for uid in uids:
        paths = list(logs_dir.glob(f'*-{uid}.log')) if logs_dir.exists() else []
        offsets[uid] = paths[0].stat().st_size if paths else 0
    return offsets


def operator_restart_increases(trial):
    watch_paths = list((OUT / 'operator-logs').rglob('operator-watch.jsonl'))
    if not watch_paths:
        return []
    before = {pod.get('uid'): pod.get('container_restarts', {})
              for pod in trial.get('pre_operator_pods', []) if pod.get('uid')}
    start = trial.get('started_monotonic_ns', 0) / 1e9
    end = trial.get('finished_monotonic_ns', time.monotonic_ns()) / 1e9
    maxima = {}
    for path in watch_paths:
        try:
            with path.open(encoding='utf-8', errors='replace') as stream:
                for line in stream:
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    received = event.get('received_monotonic')
                    if received is None or not start <= received <= end:
                        continue
                    uid = event.get('pod_uid')
                    if not uid:
                        continue
                    for container, details in (event.get('containers') or {}).items():
                        count = details.get('restart_count', 0)
                        key = (uid, container)
                        maxima[key] = max(maxima.get(key, 0), count)
        except OSError:
            continue
    increases = []
    for (uid, container), count in maxima.items():
        original = before.get(uid, {}).get(container, 0)
        if count > original:
            increases.append({'pod_uid': uid, 'container': container,
                              'restart_count_before': original,
                              'restart_count_observed': count})
    return increases


class PodWatch:
    """Maintain the current DB Pod state from a reconnecting Kubernetes watch."""
    def __init__(self, base, log):
        self.base = base
        self.log = log
        self.cv = threading.Condition()
        self.pods = {}
        self.initialized = threading.Event()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name='db-pod-watch', daemon=True)
        self.last_error = None
        self.response_lock = threading.Lock()
        self.response = None

    def start(self):
        self.thread.start()
        if not self.initialized.wait(20):
            raise TimeoutError('Pod watch did not complete its initial list')

    def close(self):
        self.stop.set()
        with self.response_lock:
            if self.response is not None:
                try:
                    self.response.close()
                except Exception:
                    pass
        with self.cv:
            self.cv.notify_all()
        self.thread.join(timeout=8)
        if self.thread.is_alive():
            raise RuntimeError('Pod watch thread did not stop after closing its HTTP stream')

    def _list(self):
        path = f'/api/v1/namespaces/{DB_NAMESPACE}/pods'
        result = api_json(self.base, path, {'labelSelector': f'cnpg.io/cluster={DB_CLUSTER}'})
        received = time.monotonic_ns()
        with self.cv:
            self.pods = {pod.get('metadata', {}).get('name'): {
                'pod': pod, 'received_ns': received} for pod in result.get('items', [])}
            self.cv.notify_all()
        self.log.emit('pod_watch_list', resource_version=result.get('metadata', {}).get('resourceVersion'),
                      pods=[pod_identity(pod) for pod in result.get('items', [])])
        self.initialized.set()
        return result.get('metadata', {}).get('resourceVersion', '')

    def _run(self):
        path = f'/api/v1/namespaces/{DB_NAMESPACE}/pods'
        resource_version = ''
        backoff = 0.25
        while not self.stop.is_set():
            try:
                if not resource_version:
                    resource_version = self._list()
                params = {'labelSelector': f'cnpg.io/cluster={DB_CLUSTER}',
                          'watch': 'true', 'allowWatchBookmarks': 'true',
                          'timeoutSeconds': '5', 'resourceVersion': resource_version}
                request_url = self.base + path + '?' + urllib.parse.urlencode(params)
                request = urllib.request.Request(request_url, headers={'Accept': 'application/json'})
                with urllib.request.urlopen(request, timeout=8) as response:
                    with self.response_lock:
                        self.response = response
                    for raw_line in response:
                        if self.stop.is_set():
                            break
                        if not raw_line.strip():
                            continue
                        received = time.monotonic_ns()
                        frame = json.loads(raw_line)
                        event_type = frame.get('type')
                        pod = frame.get('object') or {}
                        metadata = pod.get('metadata') or {}
                        name = metadata.get('name')
                        resource_version = metadata.get('resourceVersion', resource_version)
                        if event_type == 'BOOKMARK':
                            continue
                        with self.cv:
                            if event_type == 'DELETED':
                                self.pods.pop(name, None)
                            elif name:
                                self.pods[name] = {'pod': pod, 'received_ns': received}
                            self.cv.notify_all()
                        if name:
                            labels = metadata.get('labels') or {}
                            self.log.emit('pod_watch_event', watch_type=event_type,
                                          received_ns=received, pod_name=name,
                                          pod_uid=metadata.get('uid'),
                                          resource_version=resource_version,
                                          ready=ready(pod), deleting=bool(metadata.get('deletionTimestamp')),
                                          action_label=labels.get(LABEL))
                with self.response_lock:
                    self.response = None
                backoff = 0.25
            except urllib.error.HTTPError as error:
                with self.response_lock:
                    self.response = None
                if self.stop.is_set():
                    break
                self.last_error = f'HTTP {error.code}'
                if error.code == 410:
                    resource_version = ''
                else:
                    self.log.emit('pod_watch_error', error=self.last_error)
                    self.stop.wait(backoff)
                    backoff = min(backoff * 2, 5)
            except Exception as error:
                with self.response_lock:
                    self.response = None
                if self.stop.is_set():
                    break
                self.last_error = f'{type(error).__name__}: {error}'
                self.log.emit('pod_watch_error', error=self.last_error)
                resource_version = ''
                self.stop.wait(backoff)
                backoff = min(backoff * 2, 5)

    def completion_time(self, value):
        with self.cv:
            active = [item for item in self.pods.values()
                      if not item['pod'].get('metadata', {}).get('deletionTimestamp')
                      and ready(item['pod'])]
            if len(active) != EXPECTED_DB_PODS:
                return None
            if not all((item['pod'].get('metadata', {}).get('labels') or {}).get(LABEL) == value
                       for item in active):
                return None
            return max(item['received_ns'] for item in active)

    def wait_for(self, value, accepted_ns, timeout_ns, stopping):
        with self.cv:
            while not stopping.is_set():
                completed_ns = self.completion_time(value)
                if completed_ns is not None:
                    # The API response can arrive after the Pod watch event even
                    # though the PATCH was already committed. Clamp to the
                    # client-observed acceptance time instead of reporting a
                    # negative action age or waiting for an unrelated event.
                    return completed_ns, completed_ns < accepted_ns
                remaining = (timeout_ns - time.monotonic_ns()) / 1_000_000_000
                if remaining <= 0:
                    return None
                self.cv.wait(timeout=min(remaining, 0.1))
        return None


def cluster_endpoint():
    return (f'/apis/postgresql.cnpg.io/v1/namespaces/{DB_NAMESPACE}/clusters/{DB_CLUSTER}')


SCALE_METRICS = {
    'process_resident_memory_bytes', 'process_cpu_seconds_total',
    'go_memstats_heap_alloc_bytes', 'go_memstats_heap_inuse_bytes', 'go_goroutines',
}


class DatabaseDegraded(RuntimeError):
    pass


class ProbeActionTimeout(TimeoutError):
    pass


def scrape_operator_metrics(base, pods):
    samples = []
    for pod in pods:
        metadata = pod.get('metadata', {})
        name, uid = metadata.get('name'), metadata.get('uid')
        sample = {'pod_name': name, 'pod_uid': uid, 'scraped_utc': utc_now(),
                  'metrics': {}, 'error': None}
        if not name:
            sample['error'] = 'Pod has no name'
            samples.append(sample)
            continue
        path = (f'/api/v1/namespaces/{OPERATOR_NAMESPACE}/pods/'
                f'{name}:8080/proxy/metrics')
        try:
            with urllib.request.urlopen(base + path, timeout=3) as response:
                for line in response.read().decode('utf-8', errors='replace').splitlines():
                    if not line or line.startswith('#'):
                        continue
                    metric = line.split('{', 1)[0].split(' ', 1)[0]
                    if metric not in SCALE_METRICS:
                        continue
                    try:
                        sample['metrics'][metric] = float(line.rsplit(' ', 1)[-1])
                    except ValueError:
                        continue
        except Exception as error:
            sample['error'] = f'{type(error).__name__}: {error}'
        samples.append(sample)
    return samples


def database_health_snapshot(base, watch):
    snapshot = {'sampled_utc': utc_now(), 'healthy': None}
    try:
        cluster = api_json(base, cluster_endpoint(), timeout=3)
        status = cluster.get('status', {})
        spec = cluster.get('spec', {})
        with watch.cv:
            pods = [entry['pod'] for entry in watch.pods.values()]
        active = [pod for pod in pods
                  if not pod.get('metadata', {}).get('deletionTimestamp')]
        ready_pods = [pod for pod in active if ready(pod)]
        snapshot.update({
            'desired_instances': spec.get('instances'),
            'ready_instances': status.get('readyInstances'),
            'current_primary': status.get('currentPrimary'),
            'ready_pod_count': len(ready_pods),
            'pods': [pod_identity(pod) for pod in active],
            'healthy': (spec.get('instances') == EXPECTED_DB_PODS
                        and status.get('readyInstances') == EXPECTED_DB_PODS
                        and len(ready_pods) == EXPECTED_DB_PODS),
        })
    except Exception as error:
        snapshot['error'] = f'{type(error).__name__}: {error}'
    return snapshot


def submit_action(base, value, timeout=10):
    url = base + cluster_endpoint()
    payload = json.dumps({'spec': {'inheritedMetadata': {'labels': {LABEL: value}}}}).encode()
    request = urllib.request.Request(url, data=payload, method='PATCH',
                                     headers={'Content-Type': 'application/merge-patch+json',
                                              'Accept': 'application/json'})
    started_ns = time.monotonic_ns()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.loads(response.read())
            accepted_ns = time.monotonic_ns()
            return {'accepted': True, 'started_ns': started_ns, 'accepted_ns': accepted_ns,
                    'admission_seconds': (accepted_ns - started_ns) / 1e9,
                    'cluster_generation': body.get('metadata', {}).get('generation'),
                    'resource_version': body.get('metadata', {}).get('resourceVersion')}
    except urllib.error.HTTPError as error:
        detail = error.read().decode('utf-8', errors='replace')[:1500]
        accepted_ns = time.monotonic_ns()
        return {'accepted': False, 'started_ns': started_ns, 'accepted_ns': accepted_ns,
                'admission_seconds': (accepted_ns - started_ns) / 1e9,
                'http_status': error.code, 'error': detail}
    except Exception as error:
        accepted_ns = time.monotonic_ns()
        return {'accepted': False, 'started_ns': started_ns, 'accepted_ns': accepted_ns,
                'admission_seconds': (accepted_ns - started_ns) / 1e9,
                'error': f'{type(error).__name__}: {error}'}


class ProbeLoop:
    def __init__(self, base, watch, log, trial):
        self.base = base
        self.watch = watch
        self.log = log
        self.trial = trial
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name='continuous-probes', daemon=True)
        self.lock = threading.Lock()
        self.actions = []
        self.reason = None
        self.pending = None

    def start(self):
        self.thread.start()

    def completed_count(self):
        with self.lock:
            return sum(item.get('status') == 'completed' for item in self.actions)

    def completed_after(self, after_ns):
        with self.lock:
            return len({item.get('value') for item in self.actions
                        if item.get('status') == 'completed'
                        and item.get('completed_monotonic_ns', 0) >= after_ns
                        and item.get('value')})

    def close(self):
        self.stop.set()
        with self.watch.cv:
            self.watch.cv.notify_all()
        self.thread.join(timeout=15)

    def _run(self):
        action_number = 0
        while not self.stop.is_set() and time.monotonic() < RUN_DEADLINE:
            action_number += 1
            value = str(time.time_ns())
            probe = {'number': action_number, 'value': value,
                     'start_ns': time.monotonic_ns(), 'start_utc': utc_now()}
            self.log.emit('action_submit_started', action_number=action_number,
                          value=value, submit_utc=probe['start_utc'],
                          submit_monotonic_ns=probe['start_ns'],
                          phase=self._phase(probe['start_ns']))
            result = submit_action(self.base, value)
            probe.update(result)
            probe['api_request_started_monotonic_ns'] = result['started_ns']
            probe['accepted_utc'] = utc_now()
            probe['accepted_monotonic_ns'] = result['accepted_ns']
            probe['phase_at_acceptance'] = self._phase(result['accepted_ns'])
            if not result['accepted']:
                probe['status'] = 'rejected'
                self.log.emit('action_rejected', action_number=action_number, value=value,
                              admission_seconds=result['admission_seconds'],
                              http_status=result.get('http_status'), error=result.get('error'))
                self._store(probe)
                self.stop.wait(ARGS.probe_interval)
                continue

            self.pending = probe
            self.log.emit('action_accepted', action_number=action_number, value=value,
                          admission_seconds=result['admission_seconds'],
                          cluster_generation=result.get('cluster_generation'),
                          resource_version=result.get('resource_version'),
                          accepted_utc=probe['accepted_utc'],
                          accepted_monotonic_ns=result['accepted_ns'])
            end_ns = result['accepted_ns'] + int(ARGS.action_timeout * 1e9)
            completion = self.watch.wait_for(value, result['accepted_ns'], end_ns, self.stop)
            if completion is not None:
                completed_ns, observed_before_response = completion
                action_completed_ns = max(completed_ns, result['accepted_ns'])
                probe['status'] = 'completed'
                probe['completed_monotonic_ns'] = action_completed_ns
                probe['completion_watch_received_ns'] = completed_ns
                probe['watch_observed_before_api_response'] = observed_before_response
                probe['completed_utc_observed'] = utc_now()
                probe['action_latency_seconds'] = (action_completed_ns - result['accepted_ns']) / 1e9
                probe['submit_to_completion_seconds'] = (
                    action_completed_ns - result['started_ns']) / 1e9
                probe['phase_at_completion'] = self._phase(action_completed_ns)
                trigger_ns = self.trial.get('trigger_monotonic_ns')
                probe['crossed_trigger'] = bool(trigger_ns and result['accepted_ns'] < trigger_ns <= action_completed_ns)
                recovery_ns = self.trial.get('recovery_monotonic_ns')
                probe['crossed_recovery'] = bool(recovery_ns and result['accepted_ns'] < recovery_ns <= action_completed_ns)
                self.log.emit('action_completed', action_number=action_number, value=value,
                              action_latency_seconds=probe['action_latency_seconds'],
                              completed_monotonic_ns=action_completed_ns,
                              completion_watch_received_ns=completed_ns,
                              watch_observed_before_api_response=observed_before_response,
                              phase=probe['phase_at_completion'],
                              crossed_trigger=probe['crossed_trigger'],
                              crossed_recovery=probe['crossed_recovery'])
                self._store(probe)
                self.pending = None
                delay = max(0.0, (action_completed_ns + int(ARGS.probe_interval * 1e9)
                                  - time.monotonic_ns()) / 1e9)
                self.stop.wait(delay)
            else:
                observed_end = time.monotonic_ns()
                probe['pending_age_seconds'] = (observed_end - result['accepted_ns']) / 1e9
                probe['status'] = 'timed_out' if observed_end >= end_ns else 'censored_at_trial_end'
                self.reason = probe['status']
                self.log.emit('action_no_completion', action_number=action_number, value=value,
                              status=probe['status'], pending_age_seconds=probe['pending_age_seconds'])
                self._store(probe)
                self.pending = None
                break
        if self.reason is None and time.monotonic() >= RUN_DEADLINE:
            self.reason = 'run_deadline'

    def _phase(self, when_ns):
        trigger_ns = self.trial.get('trigger_monotonic_ns')
        recovery_ns = self.trial.get('recovery_monotonic_ns')
        if trigger_ns is None or when_ns < trigger_ns:
            return 'baseline'
        if recovery_ns is None or when_ns < recovery_ns:
            return 'disruption'
        return 'post_recovery'

    def _store(self, probe):
        with self.lock:
            self.actions.append(probe)
        self.trial['actions'] = self.actions
        write_summary()


RESULTS = {'run_id': RUN_ID, 'context': ARGS.context,
           'started_utc': utc_now(), 'status': 'running', 'trials': []}


def write_summary():
    with SUMMARY_LOCK:
        RESULTS['updated_utc'] = utc_now()
        temp = OUT / 'results.json.tmp'
        temp.write_text(json.dumps(RESULTS, indent=2, sort_keys=True) + '\n')
        temp.replace(OUT / 'results.json')


def start_operator_logs():
    global OPERATOR_LOG_PROCESS, OPERATOR_LOG_STDOUT
    log_dir = OUT / 'operator-logs'
    OPERATOR_LOG_STDOUT = (OUT / 'operator-logs-driver.log').open('ab')
    script = Path(__file__).with_name('operator_logs.py')
    OPERATOR_LOG_PROCESS = subprocess.Popen(
        [sys.executable, str(script), '--context', ARGS.context,
         '--output', str(log_dir)],
        stdout=OPERATOR_LOG_STDOUT, stderr=OPERATOR_LOG_STDOUT,
        start_new_session=True)
    # Give the helper time to attach its Pod watch and per-container log streams
    # before the first measured baseline starts.
    time.sleep(2)
    if OPERATOR_LOG_PROCESS.poll() is not None:
        raise RuntimeError(f'operator_logs.py exited during startup: {OPERATOR_LOG_PROCESS.returncode}')


def stop_operator_logs():
    global OPERATOR_LOG_PROCESS, OPERATOR_LOG_STDOUT
    if OPERATOR_LOG_PROCESS is not None:
        if OPERATOR_LOG_PROCESS.poll() is None:
            try:
                os.killpg(OPERATOR_LOG_PROCESS.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                OPERATOR_LOG_PROCESS.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(OPERATOR_LOG_PROCESS.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                OPERATOR_LOG_PROCESS.wait()
        OPERATOR_LOG_PROCESS = None
    if OPERATOR_LOG_STDOUT is not None:
        OPERATOR_LOG_STDOUT.close()
        OPERATOR_LOG_STDOUT = None


def summarize_trial(trial):
    actions = trial.get('actions', [])
    completed = [item for item in actions if item.get('status') == 'completed']
    admissions = [item['admission_seconds'] for item in actions if 'admission_seconds' in item]
    latencies = [item['action_latency_seconds'] for item in completed]
    pending_ages = [item['pending_age_seconds'] for item in actions if 'pending_age_seconds' in item]
    completion_times = sorted(item['completed_monotonic_ns'] for item in completed)
    gaps = [(right - left) / 1e9 for left, right in zip(completion_times, completion_times[1:])]
    trigger_ns = trial.get('trigger_monotonic_ns')
    first_post_trigger = next((stamp for stamp in completion_times if trigger_ns and stamp >= trigger_ns), None)
    before_trigger = [stamp for stamp in completion_times if trigger_ns and stamp < trigger_ns]
    across_trigger = None
    if first_post_trigger is not None:
        prior = max(before_trigger) if before_trigger else trigger_ns
        across_trigger = (first_post_trigger - prior) / 1e9
    finished_ns = trial.get('finished_monotonic_ns')
    tail_gap = ((finished_ns - completion_times[-1]) / 1e9
                if finished_ns and completion_times else None)
    trial['metrics'] = {
        'accepted_actions': sum(item.get('accepted') is True for item in actions),
        'rejected_actions': sum(item.get('status') == 'rejected' for item in actions),
        'completed_actions': len(completed),
        'max_api_admission_seconds': max(admissions) if admissions else None,
        'mean_api_admission_seconds': sum(admissions) / len(admissions) if admissions else None,
        'max_action_latency_seconds': max(latencies) if latencies else None,
        'mean_action_latency_seconds': sum(latencies) / len(latencies) if latencies else None,
        'max_observed_pending_age_seconds': max(pending_ages) if pending_ages else None,
        'max_intercompletion_gap_seconds': max(gaps) if gaps else None,
        'max_intercompletion_gap_definition': 'largest gap between completed action observations only',
        'intercompletion_gaps_seconds': gaps,
        'last_completion_to_trial_end_lower_bound_seconds': tail_gap,
        'max_observed_gap_lower_bound_seconds': max(
            [gap for gap in gaps] + ([tail_gap] if tail_gap is not None else []), default=None),
        'prior_completion_to_first_post_trigger_completion_seconds': across_trigger,
        'max_disruption_action_latency_seconds': max(
            (item['action_latency_seconds'] for item in completed
             if item.get('phase_at_acceptance') == 'disruption'), default=None),
    }


def lease_holder():
    lease = kube_json(['-n', OPERATOR_NAMESPACE, 'get', 'lease', LEASE])
    holder = lease.get('spec', {}).get('holderIdentity') or ''
    if not holder:
        raise RuntimeError('operator Lease has no holderIdentity')
    return holder.rsplit('_', 1)[0]


def trigger_trial(scenario, pre_pods, trial, log, base=None, namespace_value=None):
    trigger_pods = operator_pods()
    trial['operator_uids_at_trigger'] = sorted(
        pod.get('metadata', {}).get('uid') for pod in trigger_pods
        if pod.get('metadata', {}).get('uid'))
    if scenario == 'rollout-restart':
        trial['trigger_utc'] = utc_now()
        trial['trigger_monotonic_ns'] = time.monotonic_ns()
        trial['trigger_monotonic'] = trial['trigger_monotonic_ns'] / 1e9
        trial['trigger_started_utc'] = trial['trigger_utc']
        result = kubectl(['-n', OPERATOR_NAMESPACE, 'rollout', 'restart',
                          f'deployment/{DEPLOYMENT}'])
        trial['trigger_kind'] = 'deployment_rollout_restart'
        trial['trigger_response'] = result.stdout.strip()
    elif scenario == 'leader-delete':
        holder = lease_holder()
        leader = next((pod for pod in trigger_pods
                       if pod.get('metadata', {}).get('name') == holder), None)
        if not leader:
            raise RuntimeError(f'Lease holder {holder!r} did not identify a current operator Pod')
        trial['trigger_leader'] = pod_identity(leader)
        trial['trigger_utc'] = utc_now()
        trial['trigger_monotonic_ns'] = time.monotonic_ns()
        trial['trigger_monotonic'] = trial['trigger_monotonic_ns'] / 1e9
        trial['trigger_started_utc'] = trial['trigger_utc']
        result = kubectl(['-n', OPERATOR_NAMESPACE, 'delete', 'pod', holder, '--wait=false'])
        trial['trigger_kind'] = 'graceful_operator_leader_delete'
        trial['trigger_response'] = result.stdout.strip()
        trial['target_operator_pod_uid'] = leader.get('metadata', {}).get('uid')
        trial['target_operator_pod_name'] = holder
    elif scenario == 'namespace-scope-update':
        if base is None or namespace_value is None:
            raise ValueError('namespace-scope-update requires an API proxy and target scope')
        trial['trigger_utc'] = utc_now()
        trial['trigger_monotonic_ns'] = time.monotonic_ns()
        trial['trigger_monotonic'] = trial['trigger_monotonic_ns'] / 1e9
        trial['trigger_started_utc'] = trial['trigger_utc']
        try:
            patch_result = patch_watch_namespaces(base, namespace_value)
        except Exception as error:
            trial['scope_patch_error'] = f'{type(error).__name__}: {error}'
            detail = str(error).lower()
            http_status = getattr(error, 'code', None)
            trial['scope_patch_failure_class'] = (
                'namespace_environment_limit'
                if http_status == 413 or any(token in detail for token in
                    ('request entity too large', 'argument list too long', 'e2big',
                     'environment variable too large', 'environment list too large'))
                else 'scope_patch_failed')
            trial['trigger_kind'] = 'watch_namespace_update'
            trial['namespace_scope_value_characters'] = len(namespace_value)
            trial['namespace_scope_value_utf8_bytes'] = len(namespace_value.encode())
            log.emit('namespace_scope_patch_failed',
                     error_type=type(error).__name__, error=str(error),
                     target_namespace_count=trial.get('namespace_count'),
                     target_scope_characters=len(namespace_value))
            raise
        trial['trigger_kind'] = 'watch_namespace_update'
        trial['trigger_response'] = patch_result
        trial['namespace_scope_value_characters'] = patch_result['namespace_value_characters']
        trial['namespace_scope_value_utf8_bytes'] = patch_result['namespace_value_utf8_bytes']
    else:
        raise ValueError(f'unknown scenario: {scenario}')
    trial['trigger_response_monotonic_ns'] = time.monotonic_ns()
    if scenario == 'namespace-scope-update':
        trial['trigger_request_seconds'] = patch_result['request_seconds']
    else:
        trial['trigger_request_seconds'] = (
            trial['trigger_response_monotonic_ns'] - trial['trigger_monotonic_ns']) / 1e9
    log.emit('disruption_triggered', scenario=scenario, trigger_kind=trial['trigger_kind'],
             trigger_utc=trial['trigger_utc'], trigger_monotonic_ns=trial['trigger_monotonic_ns'],
             response=trial.get('trigger_response'), target_operator_pod=trial.get('trigger_leader'),
             previous_namespace_count=trial.get('previous_namespace_count'),
             namespace_count=trial.get('namespace_count'),
             namespace_scope_value_characters=trial.get('namespace_scope_value_characters'))


def wait_recovery(scenario, expected_replicas, old_uids, target_uid, timeout,
                  trial=None, health_check=None):
    deadline = min(time.monotonic() + timeout, RUN_DEADLINE)
    last = None
    while time.monotonic() < deadline and not STOPPING.is_set():
        if health_check is not None:
            health_check()
        deploy = deployment()
        pods = operator_pods(deploy)
        active = [pod for pod in pods if not pod.get('metadata', {}).get('deletionTimestamp')]
        status = deploy.get('status', {})
        meta = deploy.get('metadata', {})
        spec = deploy.get('spec', {})
        stable = (spec.get('replicas', 1) == expected_replicas
                  and (status.get('observedGeneration') or 0) >= (meta.get('generation') or 0)
                  and status.get('updatedReplicas', 0) == expected_replicas
                  and status.get('availableReplicas', 0) == expected_replicas
                  and status.get('unavailableReplicas', 0) == 0
                  and len(active) == expected_replicas and all(ready(pod) for pod in active))
        if scenario in ('rollout-restart', 'namespace-scope-update'):
            stable = stable and all(pod.get('metadata', {}).get('uid') not in old_uids for pod in active)
        else:
            present = any(pod.get('metadata', {}).get('uid') == target_uid for pod in pods)
            stable = stable and not present
        last = {'deployment': {'generation': meta.get('generation'),
                               'observed_generation': status.get('observedGeneration'),
                               'desired_replicas': spec.get('replicas'),
                               'updated_replicas': status.get('updatedReplicas'),
                               'available_replicas': status.get('availableReplicas')},
                'pods': [pod_identity(pod) for pod in pods]}
        if stable:
            if ARGS.mode == 'scale-until-unstable':
                uids = [pod.get('metadata', {}).get('uid') for pod in active
                        if pod.get('metadata', {}).get('uid')]
                markers = cluster_worker_start_markers(
                    uids, after_utc=(trial or {}).get('trigger_utc'),
                    offsets=(trial or {}).get('operator_log_offsets_before_trigger'))
                if not markers:
                    last['cluster_worker_start_marker'] = None
                else:
                    last['cluster_worker_start_marker'] = markers[0]
                    return last
            else:
                return last
        time.sleep(0.4)
    raise TimeoutError(f'{scenario} did not recover within {timeout}s; last={last}')


def one_trial(base, count, replicas, scenario, repetition, previous_count=None):
    global ACTIVE_LOGGER
    scope_prefix = f'from-{previous_count}-to-' if scenario == 'namespace-scope-update' else ''
    slug = (f'namespaces-{scope_prefix}{count}-replicas-{replicas}-'
            f'{scenario}-trial-{repetition:02d}')
    log = EventLog(OUT / f'{slug}.jsonl')
    ACTIVE_LOGGER = log
    deploy = deployment()
    pre_pods = operator_pods(deploy)
    old_uids = {pod.get('metadata', {}).get('uid') for pod in pre_pods}
    trial = {
        'id': slug, 'scenario': scenario, 'trial': repetition,
        'desired_replicas': replicas, 'namespace_count': count,
        'namespace_names': [f'cnpg-lab-{i:04d}' for i in range(count)],
        'previous_namespace_count': previous_count,
        'event_file': log.path.name, 'started_utc': utc_now(),
        'started_monotonic_ns': time.monotonic_ns(),
        'pre_operator_uids': sorted(uid for uid in old_uids if uid),
        'pre_operator_pods': [pod_identity(pod) for pod in pre_pods],
        'pre_leader': None,
        'final_operator_uids': [], 'actions': [],
    }
    RESULTS['trials'].append(trial)
    write_summary()
    log.emit('trial_started', scenario=scenario, desired_replicas=replicas,
             namespace_count=count, pre_operator_uids=trial['pre_operator_uids'],
             pre_operator_pods=trial['pre_operator_pods'])
    watch = PodWatch(base, log)
    probe_loop = None
    health_state = {'next_sample': 0.0}

    def check_database_health(force=False):
        if ARGS.mode != 'scale-until-unstable':
            return None
        if probe_loop is not None and probe_loop.reason == 'timed_out':
            raise ProbeActionTimeout('accepted label probe did not complete before action timeout')
        now = time.monotonic()
        if not force and now < health_state['next_sample']:
            return None
        sample = database_health_snapshot(base, watch)
        trial.setdefault('database_health_samples', []).append(sample)
        log.emit('database_health_sample', sample=sample)
        write_summary()
        health_state['next_sample'] = now + SCALE_HEALTH_POLL_SECONDS
        if sample.get('healthy') is False:
            raise DatabaseDegraded(f'target DB degraded: {sample}')
        return sample

    def capture_metrics(stage):
        if ARGS.mode != 'scale-until-unstable':
            return
        pods = operator_pods()
        samples = scrape_operator_metrics(base, pods)
        trial.setdefault('operator_resource_metrics', []).append(
            {'stage': stage, 'sampled_utc': utc_now(), 'pods': samples})
        log.emit('operator_resource_metrics', stage=stage, pods=samples)
        write_summary()

    try:
        watch.start()
        check_database_health(force=True)
        initial_active = [item['pod'] for item in watch.pods.values()
                          if not item['pod'].get('metadata', {}).get('deletionTimestamp')
                          and ready(item['pod'])]
        if len(initial_active) != EXPECTED_DB_PODS:
            raise RuntimeError(f'expected {EXPECTED_DB_PODS} ready target Pods, got {len(initial_active)}')
        initial_holder = lease_holder()
        initial_leader = next((pod for pod in pre_pods
                               if pod.get('metadata', {}).get('name') == initial_holder), None)
        if not initial_leader:
            raise RuntimeError(f'Lease holder {initial_holder!r} did not identify a current operator Pod')
        trial['pre_leader'] = pod_identity(initial_leader)
        trial['pre_leader_identity'] = initial_holder
        trial['pre_leader_uid'] = initial_leader.get('metadata', {}).get('uid')
        write_summary()
        probe_loop = ProbeLoop(base, watch, log, trial)
        probe_loop.start()
        baseline_min_end = time.monotonic() + ARGS.baseline_seconds
        baseline_extra_wait = (ARGS.action_timeout
                               if ARGS.mode == 'scale-until-unstable' else 10)
        baseline_hard_end = min(RUN_DEADLINE,
                                baseline_min_end + baseline_extra_wait)
        while (time.monotonic() < baseline_min_end or probe_loop.completed_count() < 3):
            if STOPPING.is_set() or time.monotonic() >= baseline_hard_end:
                raise TimeoutError('baseline did not produce three completed probes before timeout')
            check_database_health()
            time.sleep(0.1)
        if STOPPING.is_set() or time.monotonic() >= RUN_DEADLINE:
            raise TimeoutError('run stopped before disruption trigger')

        capture_metrics('pre_trigger')
        trial['operator_log_offsets_before_trigger'] = operator_log_offsets(
            [pod.get('metadata', {}).get('uid') for pod in pre_pods
             if pod.get('metadata', {}).get('uid')])
        target_scope = ','.join(trial['namespace_names']) if scenario == 'namespace-scope-update' else None
        trigger_trial(scenario, pre_pods, trial, log, base=base,
                      namespace_value=target_scope)
        write_summary()
        target_uid = trial.get('target_operator_pod_uid')
        recovery_state = wait_recovery(scenario, replicas, old_uids, target_uid,
                                       min(ARGS.recovery_timeout, SCALE_UNSTABLE_WORKER_TIMEOUT)
                                       if ARGS.mode == 'scale-until-unstable'
                                       else ARGS.recovery_timeout,
                                       trial=trial, health_check=check_database_health)
        trial['recovery_utc'] = utc_now()
        trial['recovery_monotonic_ns'] = time.monotonic_ns()
        trial['recovery_monotonic'] = trial['recovery_monotonic_ns'] / 1e9
        trial['recovery_state'] = recovery_state
        trial['final_operator_uids'] = sorted(
            item['uid'] for item in recovery_state['pods'] if item.get('uid'))
        log.emit('operator_recovered', scenario=scenario,
                 recovery_utc=trial['recovery_utc'],
                 recovery_monotonic_ns=trial['recovery_monotonic_ns'],
                 final_operator_uids=trial['final_operator_uids'], state=recovery_state)
        capture_metrics('workers_recovered')
        write_summary()
        recovery_ns = trial['recovery_monotonic_ns']
        post_deadline_ns = recovery_ns + int(ARGS.post_seconds * 1e9)
        collection_deadline_ns = post_deadline_ns + int(ARGS.action_timeout * 1e9)
        trial['post_recovery_required_actions'] = ARGS.post_actions
        trial['post_recovery_window_seconds'] = ARGS.post_seconds
        trial['post_recovery_collection_timeout_seconds'] = ARGS.action_timeout
        trial['post_recovery_deadline_monotonic_ns'] = post_deadline_ns
        trial['post_recovery_collection_deadline_monotonic_ns'] = collection_deadline_ns
        # Always retain the full post-recovery window. If three completions have
        # not arrived by then, allow one action-timeout interval for the probe
        # loop to finish; both waits remain bounded by the whole-run deadline.
        while True:
            check_database_health()
            now_ns = time.monotonic_ns()
            completed_after_recovery = probe_loop.completed_after(recovery_ns)
            trial['post_recovery_completed_actions'] = completed_after_recovery
            write_summary()
            if now_ns >= RUN_DEADLINE_NS:
                reason = 'run_deadline'
            elif STOPPING.is_set():
                reason = 'stop_requested'
            else:
                reason = None
            if reason is not None:
                trial['post_recovery_observation_status'] = 'incomplete'
                trial['post_recovery_incomplete_reason'] = reason
                trial['post_recovery_observation_end_utc'] = utc_now()
                trial['status'] = 'incomplete_post_recovery'
                log.emit('post_recovery_observation_incomplete', reason=reason,
                         completed_actions=completed_after_recovery,
                         required_actions=ARGS.post_actions,
                         full_window_elapsed=now_ns >= post_deadline_ns)
                break
            if now_ns >= post_deadline_ns and completed_after_recovery >= ARGS.post_actions:
                trial['post_recovery_observation_status'] = 'complete'
                trial['post_recovery_observation_end_utc'] = utc_now()
                trial['status'] = 'completed'
                break
            if (now_ns >= post_deadline_ns and not probe_loop.thread.is_alive()
                  and completed_after_recovery < ARGS.post_actions):
                reason = probe_loop.reason or 'probe_loop_stopped_before_action_target'
            elif now_ns >= collection_deadline_ns:
                reason = 'post_action_target_timeout'
            else:
                time.sleep(0.1)
                continue
            trial['post_recovery_observation_status'] = 'incomplete'
            trial['post_recovery_incomplete_reason'] = reason
            trial['post_recovery_observation_end_utc'] = utc_now()
            trial['status'] = 'incomplete_post_recovery'
            log.emit('post_recovery_observation_incomplete', reason=reason,
                     completed_actions=completed_after_recovery,
                     required_actions=ARGS.post_actions,
                     full_window_elapsed=now_ns >= post_deadline_ns)
            break
        if trial['status'] == 'completed':
            capture_metrics('post_recovery_window')
            log.emit('post_recovery_observation_complete',
                     completed_actions=trial['post_recovery_completed_actions'],
                     required_actions=ARGS.post_actions,
                     window_seconds=ARGS.post_seconds)
    except Exception as error:
        trial['status'] = 'failed'
        trial['failure'] = {'type': type(error).__name__, 'message': str(error)}
        log.emit('trial_failed', error_type=type(error).__name__, message=str(error))
        raise
    finally:
        if probe_loop is not None:
            probe_loop.close()
            trial['probe_loop_stop_reason'] = probe_loop.reason
            trial['actions'] = probe_loop.actions
            if trial.get('recovery_monotonic_ns') is not None:
                trial['post_recovery_completed_actions'] = probe_loop.completed_after(
                    trial['recovery_monotonic_ns'])
        watch.close()
        trial['finished_utc'] = utc_now()
        trial['finished_monotonic_ns'] = time.monotonic_ns()
        if ARGS.mode == 'scale-until-unstable':
            trial['operator_restart_count_increases'] = operator_restart_increases(trial)
        summarize_trial(trial)
        write_summary()
        log.emit('trial_finished', status=trial.get('status'), metrics=trial.get('metrics'))
        log.close()
        ACTIVE_LOGGER = None


def ensure_fixture():
    dep = deployment()
    global ORIGINAL_REPLICAS, ORIGINAL_WATCH_NAMESPACE
    ORIGINAL_REPLICAS = dep.get('spec', {}).get('replicas', 1)
    if ORIGINAL_REPLICAS not in (1, 2):
        raise RuntimeError(f'expected initial Deployment replicas to be 1 or 2, got {ORIGINAL_REPLICAS}')
    ORIGINAL_WATCH_NAMESPACE = env_value(dep, 'WATCH_NAMESPACE')
    if ORIGINAL_WATCH_NAMESPACE is None:
        raise RuntimeError('operator Deployment has no WATCH_NAMESPACE environment variable')
    cluster = kube_json(['-n', DB_NAMESPACE, 'get', 'cluster', DB_CLUSTER])
    if cluster.get('spec', {}).get('instances') != EXPECTED_DB_PODS:
        raise RuntimeError(f'expected {EXPECTED_DB_PODS} target database instances')
    ready_instances = cluster.get('status', {}).get('readyInstances')
    if ready_instances != EXPECTED_DB_PODS:
        raise RuntimeError(f'target Cluster reports readyInstances={ready_instances}, expected 3')
    return dep


def restore_deployment():
    errors = []
    try:
        current = deployment()
        current_replicas = current.get('spec', {}).get('replicas', 1)
        if current_replicas != ORIGINAL_REPLICAS:
            kubectl(['-n', OPERATOR_NAMESPACE, 'scale', f'deployment/{DEPLOYMENT}',
                     f'--replicas={ORIGINAL_REPLICAS}'])
        current_value = env_value(deployment(), 'WATCH_NAMESPACE')
        if current_value != ORIGINAL_WATCH_NAMESPACE:
            if ORIGINAL_WATCH_NAMESPACE is not None:
                kubectl(['-n', OPERATOR_NAMESPACE, 'set', 'env', f'deployment/{DEPLOYMENT}',
                         'WATCH_NAMESPACE=' + ORIGINAL_WATCH_NAMESPACE])
            else:
                kubectl(['-n', OPERATOR_NAMESPACE, 'set', 'env', f'deployment/{DEPLOYMENT}',
                         '--unset=WATCH_NAMESPACE'])
        wait_deployment_ready(ORIGINAL_REPLICAS, timeout=min(ARGS.recovery_timeout, 120))
    except Exception as error:
        errors.append(str(error))
    for namespace in reversed(CREATED_NAMESPACES):
        try:
            kubectl(['delete', 'namespace', namespace, '--wait=false'], timeout=15)
        except Exception as error:
            errors.append(f'{namespace}: {error}')
    RESULTS['cleanup_errors'] = errors
    if errors:
        RESULTS['status'] = 'cleanup_failed'
    write_summary()
    return errors


def stop_proxy():
    global PROXY, PROXY_LOG
    if PROXY is not None:
        if PROXY.poll() is None:
            try:
                os.killpg(PROXY.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                PROXY.wait(timeout=3)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(PROXY.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                PROXY.wait()
        PROXY = None
    if PROXY_LOG is not None:
        PROXY_LOG.close()
        PROXY_LOG = None


def handle_stop(_signum, _frame):
    STOPPING.set()
    if ACTIVE_LOGGER is not None:
        ACTIVE_LOGGER.emit('stop_requested')


def scope_value_for_count(count):
    return ','.join(f'cnpg-lab-{index:04d}' for index in range(count))


def classify_scale_trial(trial):
    failure = trial.get('failure') or {}
    message = failure.get('message', '')
    if failure.get('type') == 'DatabaseDegraded':
        return 'database_degraded', True
    if failure.get('type') == 'ProbeActionTimeout':
        return 'probe_action_timeout', False
    if trial.get('scope_patch_error'):
        if trial.get('scope_patch_failure_class') == 'namespace_environment_limit':
            return 'namespace_environment_limit', False
        return 'scope_patch_failed', True
    if failure and ('did not recover within' in message or 'worker' in message.lower()
                    and 'timeout' in message.lower()):
        if 'environment_size_limit' in message:
            return 'namespace_environment_limit', False
        return 'operator_or_worker_recovery_timeout', True
    if failure and 'baseline' in message.lower():
        if STOPPING.is_set():
            return 'capture_stop_requested', False
        if time.monotonic() >= RUN_DEADLINE:
            return 'capture_run_deadline', False
    if trial.get('operator_restart_count_increases'):
        return 'operator_container_restart', False
    if trial.get('status') == 'incomplete_post_recovery':
        reason = trial.get('post_recovery_incomplete_reason') or trial.get('probe_loop_stop_reason')
        if reason in ('run_deadline', 'stop_requested'):
            return f'capture_{reason}', False
        return f'post_recovery_probe_incomplete:{reason or "unknown"}', False
    if trial.get('status') == 'failed':
        if 'baseline' in message.lower() or trial.get('probe_loop_stop_reason') == 'timed_out':
            return 'probe_action_timeout', False
        return f'trial_failed:{failure.get("type", "unknown")}', True
    return None, False


def run_scale_trial(base, count, replicas, scenario, repetition, previous_count=None):
    try:
        one_trial(base, count, replicas, scenario, repetition,
                  previous_count=previous_count)
    except Exception:
        trial = RESULTS['trials'][-1] if RESULTS.get('trials') else {}
        reason, fatal = classify_scale_trial(trial)
        trial['scale_stop_classification'] = {'reason': reason or 'unclassified_trial_failure',
                                              'fatal_recovery_failure': fatal}
        write_summary()
        return False, reason or 'unclassified_trial_failure', fatal
    trial = RESULTS['trials'][-1]
    reason, fatal = classify_scale_trial(trial)
    if reason:
        trial['scale_stop_classification'] = {'reason': reason,
                                              'fatal_recovery_failure': fatal}
        write_summary()
        return False, reason, fatal
    return True, None, False


def reset_scale_scope(base, replicas, target_count=3):
    """Return to the prior healthy scope and requested replica count between paired cases."""
    target = scope_value_for_count(target_count)
    reset = {'replicas': replicas, 'target_namespace_count': target_count,
             'started_utc': utc_now(), 'started_monotonic_ns': time.monotonic_ns()}
    RESULTS.setdefault('scale_resets', []).append(reset)
    try:
        current = env_value(deployment(), 'WATCH_NAMESPACE')
        reset['from_namespace_value_characters'] = len(current or '')
        current_replicas = deployment().get('spec', {}).get('replicas', 1)
        if current_replicas != replicas:
            kubectl(['-n', OPERATOR_NAMESPACE, 'scale', f'deployment/{DEPLOYMENT}',
                     f'--replicas={replicas}'])
            wait_deployment_ready(replicas, timeout=SCALE_UNSTABLE_WORKER_TIMEOUT)
        current = env_value(deployment(), 'WATCH_NAMESPACE')
        if current == target:
            reset.update({'status': 'already_at_target', 'finished_utc': utc_now(),
                          'final_operator_uids': [pod.get('metadata', {}).get('uid')
                                                  for pod in operator_pods()
                                                  if pod.get('metadata', {}).get('uid')]})
            write_summary()
            return True
        pods = operator_pods()
        old_uids = {pod.get('metadata', {}).get('uid') for pod in pods
                    if pod.get('metadata', {}).get('uid')}
        offsets = operator_log_offsets(old_uids)
        trigger_utc = utc_now()
        reset['from_namespace_value_characters'] = len(current or '')
        reset['target_namespace_value_characters'] = len(target)
        reset['target_namespace_value_utf8_bytes'] = len(target.encode())
        patch = patch_watch_namespaces(base, target)
        reset['patch'] = patch
        _, ready_pods = wait_deployment_ready(
            replicas, old_uids=old_uids, timeout=SCALE_UNSTABLE_WORKER_TIMEOUT,
            require_new=True)
        new_uids = [pod.get('metadata', {}).get('uid') for pod in ready_pods
                    if pod.get('metadata', {}).get('uid')]
        deadline = min(time.monotonic() + SCALE_UNSTABLE_WORKER_TIMEOUT, RUN_DEADLINE)
        marker = []
        while time.monotonic() < deadline:
            marker = cluster_worker_start_markers(new_uids, trigger_utc, offsets)
            if marker:
                break
            time.sleep(0.4)
        if not marker:
            raise TimeoutError('prior-scope reset reached Ready but no Cluster workers started')
        cluster = kube_json(['-n', DB_NAMESPACE, 'get', 'cluster', DB_CLUSTER])
        pods_state = kube_json(['-n', DB_NAMESPACE, 'get', 'pods', '-l',
                                f'cnpg.io/cluster={DB_CLUSTER}'])['items']
        db_healthy = (cluster.get('spec', {}).get('instances') == EXPECTED_DB_PODS
                      and cluster.get('status', {}).get('readyInstances') == EXPECTED_DB_PODS
                      and sum(ready(pod) for pod in pods_state) == EXPECTED_DB_PODS)
        reset['database_health'] = {'ready_instances': cluster.get('status', {}).get('readyInstances'),
                                    'ready_pods': sum(ready(pod) for pod in pods_state),
                                    'healthy': db_healthy}
        if not db_healthy:
            raise DatabaseDegraded('database did not remain healthy during prior-scope reset')
        reset.update({'status': 'completed', 'worker_start_marker': marker[0],
                      'final_operator_uids': new_uids, 'finished_utc': utc_now(),
                      'finished_monotonic_ns': time.monotonic_ns()})
        write_summary()
        return True
    except Exception as error:
        reset.update({'status': 'failed',
                      'failure': {'type': type(error).__name__, 'message': str(error)},
                      'finished_utc': utc_now(),
                      'finished_monotonic_ns': time.monotonic_ns()})
        write_summary()
        return False


def run_scale_mode(base):
    baseline_scope = scope_value_for_count(3)
    current_scope = env_value(deployment(), 'WATCH_NAMESPACE')
    if current_scope != baseline_scope:
        raise RuntimeError('scale mode requires a healthy three-namespace starting scope: '
                           'WATCH_NAMESPACE must equal cnpg-lab-0000..cnpg-lab-0002')
    ensure_namespaces(3, base=base)
    RESULTS.update({
        'scale_counts': ARGS.scale_counts,
        'scale_replicas': ARGS.scale_replicas,
        'scale_repeat': ARGS.repeat,
        'scale_order': 'namespace_count_then_replica_count; stop at first instability',
        'scale_leader_delete_enabled': ARGS.scale_leader_delete,
        'scale_definition': {
            'worker_recovery': 'Deployment Ready with expected UIDs and a fresh Cluster Starting workers log marker',
            'probe_recovery': f'{ARGS.post_seconds}s after worker recovery plus {ARGS.post_actions} completed distinct label updates',
            'controller_or_action_instability': ['database readyInstances or ready Pod count drops below 3',
                         'operator container restartCount increases',
                         'no Deployment/Cluster-worker recovery within 120s',
                         'an accepted probe does not complete within its action timeout',
                         'post-recovery action target is not met within the bounded collection window'],
            'non_capacity_stops': ['WATCH_NAMESPACE patch rejected by API/runtime environment-size limit',
                                   'whole-run deadline or external stop request'],
            'scope_limit_note': 'Large WATCH_NAMESPACE values may hit Kubernetes/runtime environment-size limits; these are recorded separately from controller capacity.',
        },
        'scale_steps': [], 'scale_outcomes': {}, 'scale_resets': [],
    })
    for replicas in ARGS.scale_replicas:
        RESULTS['scale_outcomes'][str(replicas)] = {'status': 'pending', 'levels': []}
    write_summary()
    stop_reason = None
    stop_category = None
    stop_count = None
    stop_replicas = None
    fatal_recovery_failure = False
    measurement_error_stop = False

    def record_failure(run, count, replicas, reason, fatal):
        nonlocal stop_reason, stop_category, stop_count, stop_replicas
        nonlocal fatal_recovery_failure, measurement_error_stop
        run['status'] = 'stopped'
        run['stop_reason'] = reason
        run['finished_utc'] = utc_now()
        stop_reason, stop_count, stop_replicas = reason, count, replicas
        if reason == 'namespace_environment_limit':
            stop_category = 'scope_environment_limit'
            RESULTS['scale_outcomes'][str(replicas)].update({
                'status': 'scope_limit_reached', 'first_scope_limit_namespace_count': count,
                'reason': reason})
        elif reason.startswith('capture_'):
            stop_category = reason
            RESULTS['scale_outcomes'][str(replicas)].update({
                'status': 'deadline_reached' if reason == 'capture_run_deadline' else 'interrupted',
                'incomplete_at_namespace_count': count, 'reason': reason})
        elif reason in ('scope_patch_failed', 'unclassified_trial_failure'):
            stop_category = 'measurement_error'
            measurement_error_stop = True
            RESULTS['scale_outcomes'][str(replicas)].update({
                'status': 'measurement_error', 'failed_namespace_count': count,
                'reason': reason})
        else:
            stop_category = 'controller_or_action_instability'
            fatal_recovery_failure = fatal
            RESULTS['scale_outcomes'][str(replicas)].update({
                'status': 'unstable', 'first_unstable_namespace_count': count,
                'reason': reason})
        RESULTS['scale_stop'] = {'namespace_count': count, 'replicas': replicas,
                                 'category': stop_category, 'reason': reason,
                                 'fatal_recovery_failure': fatal}
        write_summary()

    for count_index, count in enumerate(ARGS.scale_counts):
        if STOPPING.is_set() or time.monotonic() >= RUN_DEADLINE:
            break
        previous_count = (ARGS.scale_counts[count_index - 1] if count_index
                          else (3 if count != 3 else None))
        step = {'namespace_count': count, 'previous_healthy_namespace_count': previous_count,
                'started_utc': utc_now(), 'started_monotonic_ns': time.monotonic_ns(),
                'namespace_setup_started_utc': utc_now(), 'replica_runs': []}
        RESULTS['scale_steps'].append(step)
        try:
            names = ensure_namespaces(count, base=base)
            scope = scope_value_for_count(count)
            step.update({'namespace_setup_finished_utc': utc_now(),
                         'namespace_setup_finished_monotonic_ns': time.monotonic_ns(),
                         'namespaces_available': len(names),
                         'namespace_value_characters': len(scope),
                         'namespace_value_utf8_bytes': len(scope.encode())})
            write_summary()
        except Exception as error:
            step.update({'status': 'setup_failed',
                         'stop_reason': f'namespace_creation_failed:{type(error).__name__}',
                         'failure': {'type': type(error).__name__, 'message': str(error)},
                         'finished_utc': utc_now()})
            stop_reason, stop_category = step['stop_reason'], 'measurement_error'
            stop_count, stop_replicas = count, None
            measurement_error_stop = True
            RESULTS['scale_stop'] = {'namespace_count': count, 'replicas': None,
                                     'category': stop_category, 'reason': stop_reason}
            write_summary()
            break

        for replica_index, replicas in enumerate(ARGS.scale_replicas):
            if STOPPING.is_set() or time.monotonic() >= RUN_DEADLINE:
                break
            run = {'replicas': replicas, 'namespace_count': count,
                   'previous_healthy_namespace_count': previous_count,
                   'trial_ids': [], 'started_utc': utc_now(),
                   'started_monotonic_ns': time.monotonic_ns()}
            step['replica_runs'].append(run)
            RESULTS['scale_outcomes'][str(replicas)]['levels'].append(run)
            current_deployment = deployment()
            current_replicas = current_deployment.get('spec', {}).get('replicas', 1)
            current_scope = env_value(current_deployment, 'WATCH_NAMESPACE')
            expected_start_scope = scope_value_for_count(previous_count or 3)
            if current_replicas != replicas or current_scope != expected_start_scope:
                if not reset_scale_scope(base, replicas, previous_count or 3):
                    record_failure(run, count, replicas, 'baseline_scope_or_replica_reset_failed', True)
                    break

            if previous_count is not None:
                okay, reason, fatal = run_scale_trial(
                    base, count, replicas, 'namespace-scope-update', 1,
                    previous_count=previous_count)
                if RESULTS.get('trials'):
                    run['scope_update_trial_id'] = RESULTS['trials'][-1]['id']
                    run['trial_ids'].append(RESULTS['trials'][-1]['id'])
                if not okay:
                    record_failure(run, count, replicas, reason, fatal)
                    break

            scenarios = ['rollout-restart']
            if ARGS.scale_leader_delete:
                scenarios.append('leader-delete')
            for scenario in scenarios:
                for repetition in range(1, ARGS.repeat + 1):
                    if STOPPING.is_set() or time.monotonic() >= RUN_DEADLINE:
                        run['status'] = 'incomplete'
                        run['stop_reason'] = 'stop_requested' if STOPPING.is_set() else 'run_deadline'
                        stop_reason, stop_category = run['stop_reason'], 'capture_' + (
                            'stop_requested' if STOPPING.is_set() else 'run_deadline')
                        stop_count, stop_replicas = count, replicas
                        RESULTS['scale_stop'] = {'namespace_count': count, 'replicas': replicas,
                                                 'category': stop_category, 'reason': stop_reason}
                        break
                    okay, reason, fatal = run_scale_trial(
                        base, count, replicas, scenario, repetition)
                    if RESULTS.get('trials'):
                        run['trial_ids'].append(RESULTS['trials'][-1]['id'])
                    if not okay:
                        record_failure(run, count, replicas, reason, fatal)
                        break
                if stop_reason is not None:
                    break
            if stop_reason is not None:
                break
            run.update({'status': 'stable', 'finished_utc': utc_now(),
                        'finished_monotonic_ns': time.monotonic_ns()})
            RESULTS['scale_outcomes'][str(replicas)].update({
                'status': 'stable_so_far', 'highest_stable_namespace_count': count})
            write_summary()
        if stop_reason is not None or STOPPING.is_set() or time.monotonic() >= RUN_DEADLINE:
            step.update({'status': 'stopped' if stop_reason else
                         ('interrupted' if STOPPING.is_set() else 'incomplete_at_deadline'),
                         'finished_utc': utc_now(),
                         'finished_monotonic_ns': time.monotonic_ns()})
            write_summary()
            break
        step.update({'status': 'stable', 'finished_utc': utc_now(),
                     'finished_monotonic_ns': time.monotonic_ns()})
        write_summary()

    if STOPPING.is_set():
        RESULTS['status'] = 'interrupted'
    elif time.monotonic() >= RUN_DEADLINE:
        RESULTS['status'] = 'deadline_reached'
    elif measurement_error_stop:
        RESULTS['status'] = 'stopped_after_scale_setup_or_api_error'
    elif stop_category == 'scope_environment_limit':
        RESULTS['status'] = 'stopped_at_scope_environment_limit'
    elif stop_category == 'controller_or_action_instability':
        RESULTS['status'] = ('stopped_after_unrecoverable_instability'
                             if fatal_recovery_failure else 'stopped_at_first_instability')
    elif stop_reason:
        RESULTS['status'] = 'stopped_before_scale_limit'
    else:
        RESULTS['status'] = 'scale_limit_reached'
    write_summary()


def main():
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    RESULTS.update({'mode': ARGS.mode, 'namespace_counts': ARGS.namespace_counts,
                    'repeat': ARGS.repeat, 'disposable': ARGS.disposable,
                    'baseline_seconds': ARGS.baseline_seconds, 'post_seconds': ARGS.post_seconds,
                    'probe_interval_seconds': ARGS.probe_interval,
                    'action_timeout_seconds': ARGS.action_timeout,
                    'recovery_timeout_seconds': ARGS.recovery_timeout,
                    'deadline_seconds': ARGS.deadline_seconds,
                    'post_recovery_required_actions': ARGS.post_actions,
                    'status': 'running'})
    write_summary()
    try:
        ensure_fixture()
        start_operator_logs()
        base = start_proxy()
        if ARGS.mode == 'scale-until-unstable':
            run_scale_mode(base)
        else:
            max_count = max(ARGS.namespace_counts)
            all_names = ensure_namespaces(max_count)
            RESULTS['created_namespaces'] = CREATED_NAMESPACES[:]
            RESULTS['namespaces_available'] = len(all_names)
            write_summary()
            # The direct API probe path is measured; kubectl is reserved for
            # controlled disruption and recovery state, pinned to this context.
            baseline_count = ARGS.namespace_counts[0]
            for count in ARGS.namespace_counts:
                if STOPPING.is_set() or time.monotonic() >= RUN_DEADLINE:
                    break
                namespace_value = scope_value_for_count(count)
                if set_watch_namespaces(namespace_value):
                    current_replicas = deployment().get('spec', {}).get('replicas', ORIGINAL_REPLICAS)
                    wait_deployment_ready(current_replicas)
                for replicas in (1, 2):
                    if STOPPING.is_set() or time.monotonic() >= RUN_DEADLINE:
                        break
                    if deployment().get('spec', {}).get('replicas', 1) != replicas:
                        kubectl(['-n', OPERATOR_NAMESPACE, 'scale', f'deployment/{DEPLOYMENT}',
                                 f'--replicas={replicas}'])
                    wait_deployment_ready(replicas)
                    scenarios = (('rollout-restart', 'leader-delete')
                                 if count == baseline_count else ('rollout-restart',))
                    for scenario in scenarios:
                        for repetition in range(1, ARGS.repeat + 1):
                            if STOPPING.is_set() or time.monotonic() + 30 >= RUN_DEADLINE:
                                break
                            wait_deployment_ready(replicas, timeout=min(60, ARGS.recovery_timeout))
                            try:
                                one_trial(base, count, replicas, scenario, repetition)
                            except Exception:
                                RESULTS['status'] = 'failed_early'
                                write_summary()
                                raise
            if RESULTS.get('status') == 'running':
                expected_trials = sum(2 * ARGS.repeat * (2 if count == baseline_count else 1)
                                      for count in ARGS.namespace_counts)
                if len(RESULTS['trials']) == expected_trials and not STOPPING.is_set():
                    if any(t.get('status') == 'incomplete_post_recovery'
                           for t in RESULTS['trials']):
                        RESULTS['status'] = 'completed_with_incomplete_trials'
                    else:
                        RESULTS['status'] = 'completed'
                else:
                    RESULTS['status'] = 'interrupted' if STOPPING.is_set() else 'deadline_reached'
    except Exception as error:
        RESULTS['status'] = 'failed_early'
        RESULTS['failure'] = {'type': type(error).__name__, 'message': str(error)}
        if ACTIVE_LOGGER is not None:
            ACTIVE_LOGGER.emit('run_failed', error_type=type(error).__name__, message=str(error))
        write_summary()
    finally:
        if ARGS.disposable:
            cleanup_errors = []
            RESULTS['cleanup_skipped'] = 'disposable fixture will be removed by its owner'
            RESULTS['cleanup_errors'] = []
        else:
            cleanup_errors = restore_deployment() if ORIGINAL_REPLICAS is not None else []
        stop_operator_logs()
        stop_proxy()
        RESULTS['finished_utc'] = utc_now()
        RESULTS['elapsed_seconds'] = time.monotonic() - RUN_STARTED
        if cleanup_errors:
            RESULTS['status'] = 'cleanup_failed'
        write_summary()
    print(json.dumps({'status': RESULTS['status'], 'output': str(OUT),
                      'trials': len(RESULTS['trials']),
                      'cleanup_errors': RESULTS.get('cleanup_errors', [])}, sort_keys=True))
    successful_scale_stops = {
        'stopped_at_first_instability',
        'stopped_after_unrecoverable_instability',
        'stopped_at_scope_environment_limit',
        'scale_limit_reached',
    }
    return 0 if (RESULTS['status'] == 'completed'
                 or (ARGS.mode == 'scale-until-unstable'
                     and RESULTS['status'] in successful_scale_stops)) else 1


if __name__ == '__main__':
    raise SystemExit(main())
