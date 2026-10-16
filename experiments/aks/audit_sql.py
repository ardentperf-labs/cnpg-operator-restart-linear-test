#!/usr/bin/env python3
"""Audit SQL acknowledgements, persisted IDs, and database health offline."""
import argparse
import json
from pathlib import Path
import re


def audit(root):
    log = (root/'sql-client.log').read_text()
    acknowledged = [int(x) for x in re.findall(r'\bstarted=\d+\.\d+ (\d+)\|', log)]
    persisted = [int(x) for x in (root/'persisted-write-ids.txt').read_text().splitlines()
                 if x.strip().isdigit()]
    if not acknowledged or not persisted:
        raise ValueError('Missing SQL acknowledgement or persistence evidence')
    before = json.loads((root/'database-pods-before.json').read_text())['items']
    after = json.loads((root/'database-pods-after.json').read_text())['items']
    trials = json.loads((root/'benchmark/results.json').read_text())['trials']
    samples = [s for t in trials for s in t.get('database_health_samples', [])]
    return {'sql': {'acknowledged_ids': len(acknowledged),
                    'acknowledged_range': [min(acknowledged), max(acknowledged)],
                    'success_records': log.count('result=success'),
                    'failure_records': log.count('result=failure'),
                    'persisted_ids': len(persisted),
                    'missing_acknowledged_ids': sorted(set(acknowledged)-set(persisted)),
                    'extra_persisted_ids': sorted(set(persisted)-set(acknowledged)),
                    'snapshot_note': 'SQL log is copied before querying persisted IDs; exports are not atomic. A timed-out insert may have committed without returning an ID.'},
            'database': {'same_pod_uids': sorted(p['metadata']['uid'] for p in before)==sorted(p['metadata']['uid'] for p in after),
                         'health_samples': len(samples),
                         'unhealthy_samples': sum(s.get('healthy') is not True for s in samples),
                         'all_ready_after': all(any(c['type']=='Ready' and c['status']=='True'
                                                for c in p['status'].get('conditions', [])) for p in after),
                         'container_restarts_after': sum(c.get('restartCount', 0) for p in after
                                                        for c in p['status'].get('containerStatuses', []))}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_directory', type=Path)
    args = parser.parse_args()
    result = audit(args.run_directory)
    (args.run_directory/'sql-validation.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
