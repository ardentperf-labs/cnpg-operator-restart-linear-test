#!/usr/bin/env python3
"""Provision one disposable AKS experiment and always delete its resource groups."""
import argparse
import datetime
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time
import urllib.request
import uuid

p = argparse.ArgumentParser()
p.add_argument('--region', required=True)
p.add_argument('--sku', required=True)
p.add_argument('--vm-hourly-usd', type=float, required=True)
p.add_argument('--system-nodes', type=int, default=3)
p.add_argument('--user-sku')
p.add_argument('--user-vm-hourly-usd', type=float, default=0)
p.add_argument('--extra-user-pool', action='append', default=[], metavar='NAME:SKU:HOURLY',
               help='add a one-node user pool; repeat for additional pools')
p.add_argument('--dedicated-workload-nodes', action='store_true',
               help='label user-pool nodes and schedule image warming, operator, and PostgreSQL only there')
p.add_argument('--operator-cpu-request', default='100m')
p.add_argument('--operator-memory-request', default='100Mi')
p.add_argument('--operator-cpu-limit', default='500m')
p.add_argument('--operator-memory-limit', default='512Mi')
p.add_argument('--operator-image', default='ghcr.io/cloudnative-pg/cloudnative-pg:1.30.1@sha256:923c267ec29636db3bee20f993d0ec4973fa22998e1adad37da79e4d32b5bc07')
p.add_argument('--postgres-image', default='ghcr.io/cloudnative-pg/postgresql:18.6-system-trixie@sha256:5efb66993894055f0aa50bd94337c6c4ff2e2129f4d433c527151aebbf283fb0')
p.add_argument('--output', required=True)
p.add_argument('--repeat', type=int)
p.add_argument('--benchmark-script', default='experiments/aks/response_gap.py')
p.add_argument('--namespace-counts', help='comma-separated counts for the response-gap benchmark')
p.add_argument('--benchmark-mode', choices=('benchmark', 'scale-until-unstable'))
p.add_argument('--scale-replicas', default='1,2')
p.add_argument('--benchmark-deadline-seconds', type=int, default=1800)
p.add_argument('--benchmark-timeout', type=int, default=1960)
p.add_argument('--wall-time-seconds', type=int, default=2700)
a = p.parse_args()


def parse_pool(spec):
    fields = spec.split(':')
    if len(fields) != 3:
        p.error('--extra-user-pool must have the form NAME:SKU:HOURLY')
    name, sku, hourly = fields
    if not re.fullmatch(r'[a-z][a-z0-9]{0,11}', name):
        p.error(f'invalid AKS node-pool name: {name!r}')
    if not sku.startswith('Standard_'):
        p.error(f'invalid Azure VM SKU: {sku!r}')
    try:
        rate = float(hourly)
    except ValueError:
        p.error(f'invalid hourly rate for node pool {name!r}')
    if not math.isfinite(rate) or rate <= 0:
        p.error(f'hourly rate for node pool {name!r} must be positive')
    return {'name': name, 'sku': sku, 'nodes': 1, 'hourly_usd': rate, 'mode': 'User'}


user_pools = []
if a.user_sku:
    if not math.isfinite(a.user_vm_hourly_usd) or a.user_vm_hourly_usd < 0:
        p.error('--user-vm-hourly-usd cannot be negative or non-finite')
    user_pools.append({'name': 'workload', 'sku': a.user_sku, 'nodes': 1,
                       'hourly_usd': a.user_vm_hourly_usd or None, 'mode': 'User'})
user_pools.extend(parse_pool(spec) for spec in a.extra_user_pool)
pool_names = [pool['name'] for pool in user_pools]
if len(pool_names) != len(set(pool_names)):
    p.error('user node-pool names must be unique')
if a.system_nodes < 2:
    p.error('AKS system node pool must have at least two nodes')
if not math.isfinite(a.vm_hourly_usd) or a.vm_hourly_usd <= 0:
    p.error('--vm-hourly-usd must be positive and finite')
if a.dedicated_workload_nodes and not user_pools:
    p.error('--dedicated-workload-nodes requires at least one user node pool')
if a.repeat is not None and a.repeat < 1:
    p.error('--repeat must be positive')
if min(a.benchmark_deadline_seconds, a.benchmark_timeout, a.wall_time_seconds) <= 0:
    p.error('benchmark and wall-clock durations must be positive')
if a.benchmark_timeout >= a.wall_time_seconds:
    p.error('--benchmark-timeout must be less than --wall-time-seconds')
benchmark_script = Path(a.benchmark_script)
if not benchmark_script.is_file():
    p.error(f'benchmark script does not exist: {a.benchmark_script}')
if a.benchmark_mode and benchmark_script.name != 'response_gap.py':
    p.error('--benchmark-mode is only supported with response_gap.py')
if a.namespace_counts and benchmark_script.name != 'response_gap.py':
    p.error('--namespace-counts is only supported with response_gap.py')
if a.namespace_counts:
    try:
        namespace_counts = [int(value.strip()) for value in a.namespace_counts.split(',')]
    except ValueError:
        p.error('--namespace-counts must be comma-separated integers')
    if not namespace_counts or any(value < 1 or value > 9999 for value in namespace_counts):
        p.error('--namespace-counts values must be in the range 1..9999')
    if a.benchmark_mode == 'scale-until-unstable' and (
            namespace_counts[0] < 3
            or any(right <= left for left, right in zip(namespace_counts, namespace_counts[1:]))):
        p.error('scale-mode --namespace-counts must be strictly increasing and start at 3 or higher')
try:
    scale_replicas = [int(value.strip()) for value in a.scale_replicas.split(',')]
except ValueError:
    p.error('--scale-replicas must be comma-separated integers')
if (not scale_replicas or any(value not in (1, 2) for value in scale_replicas)
        or len(scale_replicas) != len(set(scale_replicas))):
    p.error('--scale-replicas must contain unique values from 1,2')
effective_repeat = a.repeat if a.repeat is not None else (
    2 if a.benchmark_mode == 'scale-until-unstable' else 3)

out = Path(a.output)
out.mkdir(parents=True, exist_ok=True)
run_id = 'cnpg-restart-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d') + '-' + uuid.uuid4().hex[:8]
group = run_id
node_group = run_id + '-nodes'
cluster_name = 'cnpg-restart-basic'
context = run_id
private = Path(tempfile.mkdtemp(prefix=run_id + '-'))
env = {**os.environ, 'KUBECONFIG': str(private / 'kubeconfig')}
owned = False
benchmark_process = None
started = time.monotonic()
all_pools = [{'name': 'system', 'sku': a.sku, 'nodes': a.system_nodes,
              'hourly_usd': a.vm_hourly_usd, 'mode': 'System'}] + user_pools
total_nodes = sum(pool['nodes'] for pool in all_pools)
hourly_compute = (sum(pool['nodes'] * pool['hourly_usd'] for pool in all_pools)
                  if all(pool['hourly_usd'] is not None for pool in all_pools) else None)
ledger = {'run_id': run_id, 'region': a.region, 'sku': a.sku, 'nodes': total_nodes,
          'node_count': total_nodes, 'node_pools': all_pools,
          'vm_hourly_usd': a.vm_hourly_usd, 'total_vm_hourly_usd': hourly_compute,
          'operator_resources': {
              'requests': {'cpu': a.operator_cpu_request, 'memory': a.operator_memory_request},
              'limits': {'cpu': a.operator_cpu_limit, 'memory': a.operator_memory_limit}},
          'dedicated_workload_nodes': a.dedicated_workload_nodes,
          'benchmark_mode': a.benchmark_mode, 'repeat': effective_repeat,
          'benchmark_deadline_seconds': a.benchmark_deadline_seconds,
          'benchmark_timeout_seconds': a.benchmark_timeout, 'wall_time_seconds': a.wall_time_seconds,
          'resource_group': group,
          'node_resource_group': node_group, 'cluster': cluster_name,
          'context': context, 'cnpg_version': '1.30.1',
          'postgres_image_tag': '18.6-system-trixie', 'cleanup_verified': False,
          'created_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()}
ledger['system_nodes'] = a.system_nodes
ledger['user_sku'] = a.user_sku
ledger['user_vm_hourly_usd'] = a.user_vm_hourly_usd

def save():
    (out / 'lifecycle.json').write_text(json.dumps(ledger, indent=2))

def log(message):
    print(json.dumps({'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                      'event': message, 'run_id': run_id}), flush=True)

def call(args, timeout=120, input_text=None, check=True):
    r = subprocess.run(args, input=input_text, capture_output=True, text=True,
                       timeout=timeout, env=env)
    if check and r.returncode:
        raise RuntimeError(f'{args[0]} {args[1:3]} failed: {r.stderr.strip()}')
    return r

def cloud(args, timeout=120, check=True):
    return call(['az'] + args + ['--subscription', ledger['subscription_id'], '--only-show-errors'],
                timeout=timeout, check=check)

def kube(args, timeout=120, input_text=None, check=True):
    return call(['kubectl', '--context', context, '--request-timeout=30s'] + args,
                timeout=timeout, input_text=input_text, check=check)

def apply(obj):
    return kube(['apply', '-f', '-'], input_text=json.dumps(obj))

def expired(signum, frame):
    raise TimeoutError(f'disposable AKS experiment exceeded its {a.wall_time_seconds}-second wall-clock limit')


def stop_benchmark_process():
    """Stop the benchmark process group so it cannot mutate the cluster during cleanup."""
    global benchmark_process
    if benchmark_process is None or benchmark_process.poll() is not None:
        return
    try:
        os.killpg(benchmark_process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        benchmark_process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(benchmark_process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        benchmark_process.wait(timeout=10)


def export_targeted_artifacts():
    """Export useful SQL/operator evidence, with a strict one-minute ceiling."""
    deadline = time.monotonic() + 60
    targets = [
        ('sql-client.log', ['-n', 'cnpg-lab-0000', 'logs', 'sql-client', '--timestamps'], 15),
        ('cluster-final.json', ['-n', 'cnpg-lab-0000', 'get', 'cluster', 'probe-000', '-o', 'json'], 8),
        ('database-pods-after.json', ['-n', 'cnpg-lab-0000', 'get', 'pods', '-l',
                                      'cnpg.io/podRole=instance', '-o', 'json'], 8),
        ('operator-final.log', ['-n', 'cnpg-system', 'logs', '-l',
                                'app.kubernetes.io/name=cloudnative-pg', '--since=30m', '--tail=300'], 8),
    ]
    primary = None
    for filename, args, target_timeout in targets:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            r = kube(args, timeout=min(target_timeout, max(1, int(remaining))), check=False)
            (out / filename).write_text(r.stdout if r.returncode == 0 else r.stderr)
            if filename == 'cluster-final.json' and r.returncode == 0:
                try:
                    primary = json.loads(r.stdout).get('status', {}).get('currentPrimary')
                except json.JSONDecodeError:
                    pass
        except (subprocess.TimeoutExpired, OSError) as error:
            (out / filename).write_text(str(error))
    remaining = deadline - time.monotonic()
    if primary and remaining > 0:
        try:
            r = kube(['-n', 'cnpg-lab-0000', 'exec', primary, '--', 'psql', '-d', 'app', '-X', '-qAt',
                      '-c', 'select id from restart_probe order by id'],
                     timeout=min(10, max(1, int(remaining))), check=False)
            (out / 'persisted-write-ids.txt').write_text(r.stdout if r.returncode == 0 else r.stderr)
        except (subprocess.TimeoutExpired, OSError) as error:
            (out / 'persisted-write-ids.txt').write_text(str(error))

signal.signal(signal.SIGALRM, expired)
signal.signal(signal.SIGTERM, expired)
signal.alarm(a.wall_time_seconds)
save()
try:
    ledger['subscription_id'] = json.loads(call(['az', 'account', 'show', '-o', 'json']).stdout)['id']
    for candidate in [group, node_group]:
        if cloud(['group', 'exists', '-n', candidate, '-o', 'tsv']).stdout.strip() != 'false':
            raise RuntimeError('unique experiment resource-group name already exists; refusing to touch it')
    # Both names are unique and were absent; cleanup includes partial create failures.
    owned = True
    cloud(['group', 'create', '-n', group, '-l', a.region, '--tags',
           'purpose=cnpg-restart-basic', 'run-id=' + run_id, '-o', 'none'])
    save()
    log('resource_group_created; creating_aks')
    cloud(['aks', 'create', '-g', group, '-n', cluster_name, '-l', a.region,
           '--node-resource-group', node_group, '--node-count', str(a.system_nodes),
           '--node-vm-size', a.sku, '--node-osdisk-size', '32',
           '--node-osdisk-type', 'Managed', '--os-sku', 'Ubuntu',
           '--tier', 'free', '--no-ssh-key', '--enable-managed-identity',
           '--network-plugin', 'azure', '--network-plugin-mode', 'overlay',
           '--max-pods', '50', '--load-balancer-sku', 'standard',
           '--outbound-type', 'loadBalancer', '--no-wait', '-o', 'none'], timeout=180)
    deadline = time.monotonic() + 1500
    last_notice = 0
    while time.monotonic() < deadline:
        r = cloud(['aks', 'show', '-g', group, '-n', cluster_name,
                   '--query', 'provisioningState', '-o', 'tsv'], check=False)
        state = r.stdout.strip()
        if state == 'Succeeded':
            break
        if state in ('Failed', 'Canceled'):
            failed = cloud(['rest', '--method', 'get', '--url',
                'https://management.azure.com/subscriptions/' + ledger['subscription_id'] +
                '/resourceGroups/' + group + '/providers/Microsoft.ContainerService/managedClusters/' +
                cluster_name + '?api-version=2025-10-01', '-o', 'json'], check=False)
            (out / 'failed-cluster-resource.json').write_text(failed.stdout or failed.stderr)
            if failed.returncode == 0:
                ledger['provisioning_error'] = json.loads(failed.stdout).get('properties', {}).get('status', {})
            raise RuntimeError('AKS provisioning ' + state)
        if time.monotonic() - last_notice >= 60:
            log('aks_provisioning_' + (state or 'pending'))
            last_notice = time.monotonic()
        time.sleep(10)
    else:
        raise TimeoutError('AKS did not provision within 25 minutes')
    for pool in user_pools:
        log(f"system_pool_ready; adding_user_pool_{pool['name']}")
        cloud(['aks', 'nodepool', 'add', '-g', group, '--cluster-name', cluster_name,
               '-n', pool['name'], '--mode', 'User', '--node-count', str(pool['nodes']),
               '--node-vm-size', pool['sku'], '--node-osdisk-size', '32',
               '--node-osdisk-type', 'Managed', '--os-sku', 'Ubuntu', '--max-pods', '50',
               '--labels', 'cnpg-restart-workload=true',
               '--no-wait', '-o', 'none'], timeout=180)
        deadline = time.monotonic() + 900
        last_notice = 0
        while time.monotonic() < deadline:
            state = cloud(['aks', 'nodepool', 'show', '-g', group, '--cluster-name', cluster_name,
                           '-n', pool['name'], '--query', 'provisioningState', '-o', 'tsv']).stdout.strip()
            if state == 'Succeeded':
                break
            if state in ('Failed', 'Canceled'):
                (out / 'failed-nodepool.json').write_text(cloud(
                    ['aks', 'nodepool', 'show', '-g', group, '--cluster-name', cluster_name,
                     '-n', pool['name'], '-o', 'json'], check=False).stdout)
                raise RuntimeError(f"user node pool {pool['name']} provisioning {state}")
            if time.monotonic() - last_notice >= 60:
                log(f"user_pool_{pool['name']}_provisioning_{state}")
                last_notice = time.monotonic()
            time.sleep(10)
        else:
            raise TimeoutError(f"user node pool {pool['name']} did not provision within 15 minutes")
    (out / 'aks.json').write_text(cloud(['aks', 'show', '-g', group, '-n', cluster_name, '-o', 'json']).stdout)
    cloud(['aks', 'get-credentials', '-g', group, '-n', cluster_name,
           '--file', str(private / 'kubeconfig'), '--context', context, '--overwrite-existing'])
    kube(['wait', 'node', '--all', '--for=condition=Ready', '--timeout=180s'], timeout=210)
    (out / 'nodes.json').write_text(kube(['get', 'nodes', '-o', 'json']).stdout)
    log('aks_ready; prewarming_images_on_all_nodes')
    operator_image = a.operator_image
    pg_image = a.postgres_image
    ledger['operator_image'] = operator_image
    ledger['postgres_image'] = pg_image
    save()
    namespaces = [f'cnpg-lab-{i:04d}' for i in range(3)]
    apply({'apiVersion': 'v1', 'kind': 'List', 'items': [
        {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': n}} for n in namespaces]})
    warm = {'apiVersion': 'apps/v1', 'kind': 'DaemonSet',
        'metadata': {'name': 'warm-images', 'namespace': namespaces[0]},
        'spec': {'selector': {'matchLabels': {'app': 'cnpg-warm-images'}},
          'template': {'metadata': {'labels': {'app': 'cnpg-warm-images'}}, 'spec': {
            'initContainers': [{'name': 'operator', 'image': operator_image,
                'imagePullPolicy': 'IfNotPresent', 'command': ['/manager', '--help'],
                'resources': {'requests': {'cpu': '10m', 'memory': '32Mi'}}}],
            'containers': [{'name': 'postgres', 'image': pg_image,
                'imagePullPolicy': 'IfNotPresent', 'command': ['sh', '-c', 'sleep 3600'],
                'resources': {'requests': {'cpu': '1m', 'memory': '16Mi'}}}]}}}}
    if a.dedicated_workload_nodes:
        warm['spec']['template']['spec']['nodeSelector'] = {'cnpg-restart-workload': 'true'}
    apply(warm)
    kube(['-n', namespaces[0], 'rollout', 'status', 'daemonset/warm-images', '--timeout=300s'], timeout=330)
    (out / 'warm-image-pods.json').write_text(kube(['-n', namespaces[0], 'get', 'pods', '-l',
                                                   'app=cnpg-warm-images', '-o', 'json']).stdout)
    kube(['-n', namespaces[0], 'delete', 'daemonset', 'warm-images', '--wait=true'], timeout=120)
    manifest_path = out / 'cnpg-release.yaml'
    urllib.request.urlretrieve('https://github.com/cloudnative-pg/cloudnative-pg/releases/download/v1.30.1/cnpg-1.30.1.yaml', manifest_path)
    kube(['apply', '--server-side', '-f', str(manifest_path)], timeout=120)
    patch = {'spec': {'strategy': {'type': 'RollingUpdate', 'rollingUpdate': {'maxSurge': 1, 'maxUnavailable': 0}},
        'template': {'spec': {'affinity': {'podAntiAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': [{
                'labelSelector': {'matchLabels': {'app.kubernetes.io/name': 'cloudnative-pg'}},
                'topologyKey': 'kubernetes.io/hostname'}]}},
          'containers': [{'name': 'manager', 'image': operator_image,
            'imagePullPolicy': 'IfNotPresent',
            'resources': {'requests': {'cpu': a.operator_cpu_request,
                                       'memory': a.operator_memory_request},
                          'limits': {'cpu': a.operator_cpu_limit,
                                     'memory': a.operator_memory_limit}},
            'env': [{'name': 'WATCH_NAMESPACE', 'value': ','.join(namespaces)},
                    {'name': 'OPERATOR_IMAGE_NAME', 'value': operator_image}]}]}}}}
    if a.dedicated_workload_nodes:
        patch['spec']['template']['spec']['nodeSelector'] = {'cnpg-restart-workload': 'true'}
    kube(['-n', 'cnpg-system', 'patch', 'deployment', 'cnpg-controller-manager',
          '--type=strategic', '-p', json.dumps(patch)])
    kube(['-n', 'cnpg-system', 'rollout', 'status', 'deployment/cnpg-controller-manager', '--timeout=180s'], timeout=210)
    fixture = {'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Cluster',
        'metadata': {'name': 'probe-000', 'namespace': namespaces[0]}, 'spec': {
          'instances': 3, 'imageName': pg_image, 'imagePullPolicy': 'IfNotPresent',
          'storage': {'size': '1Gi', 'storageClass': 'managed-csi'},
          'resources': {'requests': {'cpu': '50m', 'memory': '64Mi'},
                        'limits': {'memory': '256Mi'}},
          'affinity': {'enablePodAntiAffinity': True, 'podAntiAffinityType': 'required'},
          'postgresql': {'parameters': {'shared_buffers': '16MB', 'max_connections': '20',
              'work_mem': '1MB', 'maintenance_work_mem': '16MB'},
              'synchronous': {'method': 'any', 'number': 1}}}}
    if a.dedicated_workload_nodes:
        fixture['spec']['affinity']['nodeSelector'] = {'cnpg-restart-workload': 'true'}
    (out / 'fixture.json').write_text(json.dumps(fixture, indent=2))
    apply(fixture)
    kube(['-n', namespaces[0], 'wait', 'cluster/probe-000', '--for=jsonpath={.status.readyInstances}=3',
          '--timeout=360s'], timeout=390)
    (out / 'database-pods-before.json').write_text(kube(['-n', namespaces[0], 'get', 'pods', '-l',
                                    'cnpg.io/podRole=instance', '-o', 'json']).stdout)
    (out / 'operator-deployment.json').write_text(kube(['-n', 'cnpg-system', 'get', 'deployment',
                                              'cnpg-controller-manager', '-o', 'json']).stdout)
    # Load client uses Secret references; its credentials never appear in artifacts.
    sql_client = Path(__file__).with_name('sql-client.yaml').read_text().replace(
        'ghcr.io/cloudnative-pg/postgresql:18.6-system-trixie', pg_image)
    kube(['apply', '-f', '-'], input_text=sql_client)
    kube(['-n', namespaces[0], 'wait', 'pod/sql-client', '--for=condition=Ready', '--timeout=120s'], timeout=150)
    log('database_ready; starting_benchmark')
    benchmark_command = ['python3', a.benchmark_script, '--context', context,
                         '--output', str(out / 'benchmark'), '--repeat', str(effective_repeat)]
    if a.namespace_counts:
        counts_flag = '--scale-counts' if a.benchmark_mode == 'scale-until-unstable' else '--namespace-counts'
        benchmark_command += [counts_flag, a.namespace_counts]
    if a.benchmark_mode:
        benchmark_command += ['--mode', a.benchmark_mode, '--disposable',
                              '--deadline-seconds', str(a.benchmark_deadline_seconds)]
        if a.benchmark_mode == 'scale-until-unstable':
            benchmark_command += ['--scale-replicas', a.scale_replicas]
    ledger['benchmark_script'] = a.benchmark_script
    ledger['benchmark_command'] = benchmark_command
    save()
    benchmark_process = subprocess.Popen(benchmark_command, env=env, start_new_session=True)
    benchmark_timed_out = False
    try:
        benchmark_return_code = benchmark_process.wait(timeout=a.benchmark_timeout)
    except subprocess.TimeoutExpired:
        benchmark_timed_out = True
        stop_benchmark_process()
        benchmark_return_code = benchmark_process.returncode if benchmark_process.returncode is not None else 124
    ledger['benchmark_exit_code'] = benchmark_return_code
    ledger['benchmark_timed_out'] = benchmark_timed_out
    export_targeted_artifacts()
    ledger['artifacts_exported'] = True
    save()
    if benchmark_timed_out:
        raise TimeoutError(f'benchmark exceeded --benchmark-timeout={a.benchmark_timeout}')
    if benchmark_return_code:
        raise RuntimeError('benchmark failed; partial evidence exported')
    log('benchmark_finished; deleting_all_experiment_resources')
except BaseException as error:
    stop_benchmark_process()
    ledger['error'] = str(error)
    save()
    log('experiment_error; cleanup_starting')
    # Export only bounded, targeted diagnostics before resource cleanup.
    if (private / 'kubeconfig').exists() and not ledger.get('artifacts_exported'):
        export_targeted_artifacts()
        ledger['artifacts_exported'] = True
        save()
    raise
finally:
    stop_benchmark_process()
    signal.alarm(0)
    try:
        if owned:
            ledger['cleanup_started_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            save()
            log('deleting_experiment_resource_group')
            try:
                cloud(['group', 'delete', '-n', group, '--yes', '--no-wait'], timeout=180)
                deadline = time.monotonic() + 900
                last_notice = 0
                while time.monotonic() < deadline:
                    exist = {g: cloud(['group', 'exists', '-n', g, '-o', 'tsv']).stdout.strip() for g in [group, node_group]}
                    if all(v == 'false' for v in exist.values()):
                        ledger['cleanup_verified'] = True
                        break
                    # After AKS/main RG deletion, remove any remaining owned node RG.
                    if exist[group] == 'false' and exist[node_group] == 'true':
                        cloud(['group', 'delete', '-n', node_group, '--yes', '--no-wait'], timeout=180)
                    if time.monotonic() - last_notice >= 60:
                        log('cleanup_pending_' + json.dumps(exist))
                        last_notice = time.monotonic()
                    time.sleep(10)
                if not ledger['cleanup_verified']:
                    raise TimeoutError('resource groups remain after cleanup wait')
                ledger['cleanup_completed_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                ledger['total_elapsed_seconds'] = time.monotonic() - started
                ledger['vm_compute_upper_bound_usd'] = (
                    hourly_compute * ledger['total_elapsed_seconds'] / 3600
                    if hourly_compute is not None else None)
                save()
                log('cleanup_verified; both_experiment_resource_groups_absent')
            except BaseException as error:
                ledger['cleanup_error'] = str(error)
                save()
                log('cleanup_failed; manual_followup_required')
                raise
    finally:
        shutil.rmtree(private, ignore_errors=True)
