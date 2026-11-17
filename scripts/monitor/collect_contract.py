#!/usr/bin/env python3
"""Save actual bRPC samples and a metric/type inventory for release review.

Use --target client=http://host:port (and mds/cache), --source-sha BASE_SHA,
--output-dir DIR. Samples are evidence, not proof of workload correctness.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re
from urllib.request import urlopen
from urllib.parse import urlsplit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target', action='append', required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--client-stats', type=Path)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = dict(timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  source_sha=args.source_sha, runtime_acceptance=False, targets=[])
    for item in args.target:
        component, url = item.split('=', 1)
        if component not in ('client', 'mds', 'cache'):
            parser.error('target component must be client, mds or cache')
        parsed = urlsplit(url)
        if parsed.scheme not in ('http', 'https') or parsed.username or parsed.password or parsed.query or parsed.fragment:
            parser.error('target must be a plain HTTP(S) base URL without credentials')
        target = dict(component=component, url=url, samples={})
        for endpoint in ('brpc_metrics', 'vars'):
            with urlopen(url.rstrip('/')+'/'+endpoint, timeout=10) as response:
                body = response.read()
                content_type = response.headers.get('Content-Type', '')
            name = f'{component}.{endpoint}.txt'
            (args.output_dir/name).write_bytes(body)
            target['samples'][endpoint] = dict(file=name, sha256=hashlib.sha256(body).hexdigest(),
                                               bytes=len(body), content_type=content_type)
            if endpoint == 'brpc_metrics':
                text = body.decode()
                types = dict(re.findall(r'^# TYPE (\S+) (\S+)$', text, re.M))
                names = sorted(set(re.findall(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{|\s)', text, re.M)))
                target.update(metric_names=names, exported_types=types,
                              prometheus_content_type='text/plain' in content_type,
                              sample_count=sum(1 for line in text.splitlines() if line and not line.startswith('#')))
        report['targets'].append(target)
    if args.client_stats:
        body = args.client_stats.read_bytes()
        (args.output_dir/'client.stats.txt').write_bytes(body)
        report['client_stats'] = dict(file='client.stats.txt', sha256=hashlib.sha256(body).hexdigest())
    (args.output_dir/'contract-samples.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({t['component']: t['sample_count'] for t in report['targets']}, indent=2))


if __name__ == '__main__':
    main()
