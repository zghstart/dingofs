#!/usr/bin/env python3
"""Reliable bRPC target discovery and discovery-only self-monitoring.

Expected targets are operator-owned file_sd groups, independent of discovery.
Application metrics continue to use /brpc_metrics, /vars and .stats.
"""
import argparse
import configparser
import fcntl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import logging
import math
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
import time

LOG = logging.getLogger('dingofs.discovery')
COMMANDS = {'mds': ['mds', 'status'], 'client': ['fs', 'mountpoint'],
            'cache': ['cache', 'member', 'list']}
JOBS = {'mds': 'mds', 'client': 'client', 'cache': 'remotecache'}
REASONS = ('command', 'timeout', 'json', 'schema', 'business')


class DiscoveryError(Exception):
    def __init__(self, reason, message):
        super().__init__(message)
        self.reason = reason


def endpoint(host, port):
    if not isinstance(host, str) or not host or any(c.isspace() for c in host):
        raise ValueError('invalid target host')
    if isinstance(port, bool) or not isinstance(port, int) or not 0 < port <= 65535:
        raise ValueError('invalid target port')
    host = host.strip('[]')
    try:
        ip = ipaddress.ip_address(host)
        host = f'[{ip}]' if ip.version == 6 else str(ip)
    except ValueError:
        if not re.fullmatch(r'[A-Za-z0-9_.-]+', host):
            raise ValueError('invalid target hostname')
    return f'{host}:{port}'


def target_host(address):
    return address[1:address.index(']')] if address.startswith('[') else address.rsplit(':', 1)[0]


def validate_groups(groups, cluster, expected=False):
    if not isinstance(groups, list):
        raise ValueError('target groups must be an array')
    result = {}
    for group in groups:
        if not isinstance(group, dict):
            raise ValueError('target group must be an object')
        labels, addresses = group.get('labels'), group.get('targets')
        if not isinstance(labels, dict) or not isinstance(addresses, list):
            raise ValueError('target group needs labels and targets')
        component = labels.get('component')
        if component not in COMMANDS:
            raise ValueError('component must be mds, client or cache')
        if set(labels) - {'cluster', 'component', 'job', 'node', 'fs_id'}:
            raise ValueError('unsupported target labels')
        if any(not isinstance(v, str) or not v for v in labels.values()):
            raise ValueError('label values must be nonempty strings')
        if labels.get('cluster', cluster) != cluster:
            raise ValueError('inventory belongs to a different cluster')
        if expected and not labels.get('node'):
            raise ValueError('expected targets require stable node identities')
        for address in addresses:
            if not isinstance(address, str) or ':' not in address:
                raise ValueError('invalid target address')
            normalized = endpoint(target_host(address), int(address.rsplit(':', 1)[1]))
            item_labels = dict(labels, cluster=cluster, job=JOBS[component])
            item_labels.setdefault('node', target_host(normalized))
            item = {'targets': [normalized], 'labels': item_labels}
            key = (component, normalized)
            if key in result and result[key] != item:
                raise ValueError('conflicting labels for the same target')
            result[key] = item
    return [result[key] for key in sorted(result)]


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o644)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class Discovery:
    def __init__(self, target_path, state_path, cluster='default', inventory_path=None,
                 retention_seconds=3600, timeout=5, tool='dingo', runner=None, clock=time.time):
        if not cluster or not math.isfinite(retention_seconds) or retention_seconds <= 0 or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError('cluster, retention and timeout must be nonempty/positive')
        self.target_path, self.state_path = Path(target_path), Path(state_path)
        if self.target_path.resolve() == self.state_path.resolve():
            raise ValueError('target and state paths must be different')
        self.cluster, self.retention, self.timeout = cluster, retention_seconds, timeout
        self.tool, self.runner, self.clock = tool, runner or subprocess.run, clock
        self.lock = threading.Lock()
        self.state = {}
        self.published = {c: [] for c in COMMANDS}
        self.write_ok, self.write_errors = False, 0
        self.inventory_loaded = inventory_path is not None
        self.expected = validate_groups(json.loads(Path(inventory_path).read_text()), cluster, True) if inventory_path else []
        self.identities = {g['targets'][0]: g['labels'] for g in self.expected}
        if self.state_path.exists():
            saved = json.loads(self.state_path.read_text())
            if saved.get('version') != 1 or saved.get('cluster') != cluster:
                raise ValueError('discovery state version or cluster mismatch')
            self.state = saved['components']
            for component, snapshot in self.state.items():
                if component not in COMMANDS or not isinstance(snapshot, dict):
                    raise ValueError('invalid discovery state')
                timestamp = snapshot.get('last_success')
                if not isinstance(timestamp, (int, float)) or not math.isfinite(timestamp) or timestamp < 0:
                    raise ValueError('invalid last-success timestamp')
                snapshot['targets'] = validate_groups(snapshot['targets'], cluster)
                if any(g['labels']['component'] != component for g in snapshot['targets']):
                    raise ValueError('state target component mismatch')
                if any(not isinstance(n, int) or n < 0 for n in snapshot.get('errors', {}).values()):
                    raise ValueError('invalid discovery error count')
                snapshot['success'] = False
        elif self.target_path.exists() and json.loads(self.target_path.read_text()):
            # Migrate old file_sd data conservatively. Its mtime is only a
            # retention boundary, never evidence of a successful discovery.
            legacy = json.loads(self.target_path.read_text())
            for group in legacy:
                labels = dict(group.get('labels', {}))
                component = next((c for c, job in JOBS.items() if labels.get('job') == job), None)
                if component is None:
                    raise ValueError('unknown legacy target job')
                labels.update(cluster=cluster, component=component)
                converted = validate_groups([dict(targets=group['targets'], labels=labels)], cluster)
                snapshot = self.state.setdefault(component, dict(last_success=0, targets=[], errors={}, success=False,
                                                                 retained_since=self.target_path.stat().st_mtime))
                snapshot['targets'].extend(converted)
            LOG.warning('migrated legacy targets as unverified retained data; last_success remains zero')

    def fetch(self, component, mdsaddr=None):
        command = [self.tool, *COMMANDS[component], '--format=json']
        if mdsaddr:
            command.append(f'--mdsaddr={mdsaddr}')
        try:
            process = self.runner(command, capture_output=True, text=True, timeout=self.timeout, check=False)
        except subprocess.TimeoutExpired as error:
            raise DiscoveryError('timeout', 'command timed out') from error
        except OSError as error:
            raise DiscoveryError('command', 'command could not start') from error
        if process.returncode:
            raise DiscoveryError('command', f'command exited {process.returncode}')
        try:
            envelope = json.loads(process.stdout)
        except (ValueError, TypeError) as error:
            raise DiscoveryError('json', 'invalid or empty JSON') from error
        if not isinstance(envelope, dict) or not isinstance(envelope.get('error'), dict):
            raise DiscoveryError('schema', 'missing CLI error envelope')
        if envelope['error'].get('code') != 0:
            raise DiscoveryError('business', 'CLI business error')
        result = envelope.get('result')
        if not isinstance(result, dict) or not isinstance(result.get('error', {}), dict):
            raise DiscoveryError('schema', 'missing result or invalid RPC error')
        if result.get('error', {}).get('errcode', 0) != 0:
            raise DiscoveryError('business', 'RPC business error')
        field = {'mds': 'mdses', 'client': 'fs_infos', 'cache': 'members'}[component]
        # encoding/json omits zero-value protobuf fields. A real successful
        # empty response can contain only info, or even be {}. The CLI's
        # explicit error.code=0 is authoritative; unknown structures are not.
        if field not in result and set(result) - {'error', 'info'}:
            raise DiscoveryError('schema', f'missing {field}')
        if 'info' in result and not isinstance(result['info'], dict):
            raise DiscoveryError('schema', 'invalid RPC info object')
        rows = result.get(field, [])
        if not isinstance(rows, list):
            raise DiscoveryError('schema', f'{field} must be an array')
        groups = []
        try:
            for row in rows:
                if not isinstance(row, dict):
                    raise ValueError('entry must be an object')
                fs_id = None
                if component == 'client':
                    mounts = row.get('mount_points', [])
                    fs_id = row.get('fs_id')
                    if not isinstance(mounts, list):
                        raise ValueError('mount_points must be an array')
                    if mounts and (isinstance(fs_id, bool) or not str(fs_id).isdigit()):
                        raise ValueError('mounted client needs numeric fs_id')
                else:
                    mounts = [row['location']] if component == 'mds' else [row]
                for mount in mounts:
                    host = mount.get('host') if component == 'mds' else mount.get('ip')
                    address = endpoint(host, mount.get('port'))
                    labels = dict(cluster=self.cluster, component=component,
                                  job=JOBS[component], node=target_host(address))
                    known = self.identities.get(address)
                    if known:
                        if known['component'] != component:
                            raise ValueError('inventory component mismatch')
                        labels['node'] = known['node']
                    if fs_id is not None:
                        labels['fs_id'] = str(fs_id)
                        if known and known.get('fs_id', str(fs_id)) != str(fs_id):
                            raise ValueError('inventory fs_id mismatch')
                    groups.append(dict(targets=[address], labels=labels))
            return validate_groups(groups, self.cluster)
        except (KeyError, TypeError, AttributeError, ValueError) as error:
            raise DiscoveryError('schema', str(error)) from error

    def refresh(self, mdsaddr=None):
        results = {}
        for component in COMMANDS:
            try:
                results[component] = self.fetch(component, mdsaddr)
            except DiscoveryError as error:
                LOG.warning('%s discovery failed (%s): %s', component, error.reason, error)
                results[component] = error
        now = self.clock()
        with self.lock:
            for component, result in results.items():
                snapshot = self.state.setdefault(component, dict(last_success=0, targets=[], errors={}))
                snapshot['success'] = not isinstance(result, DiscoveryError)
                if snapshot['success']:
                    snapshot.update(last_success=now, targets=result)
                else:
                    errors = snapshot['errors']
                    errors[result.reason] = errors.get(result.reason, 0) + 1
            active = {c: s['targets'] if now-(s['last_success'] or s.get('retained_since', 0)) <= self.retention else []
                      for c, s in self.state.items()}
            try:
                atomic_json(self.state_path, dict(version=1, cluster=self.cluster, components=self.state))
                atomic_json(self.target_path, [g for c in sorted(active) for g in active[c]])
                self.published, self.write_ok = active, True
            except OSError:
                self.write_ok = False
                self.write_errors += 1
                LOG.exception('could not persist discovery snapshot')
            return self.write_ok and all(s['success'] for s in self.state.values())

    def metrics(self):
        lines = []
        def emit(name, help_text, kind, samples):
            full = 'dingofs_' + name
            lines.extend([f'# HELP {full} {help_text}', f'# TYPE {full} {kind}'])
            for labels, value in samples:
                text = ','.join(f'{k}={json.dumps(str(v), ensure_ascii=False)}' for k, v in sorted(labels.items()))
                lines.append(f'{full}{{{text}}} {value}')
        with self.lock:
            now, base = self.clock(), dict(cluster=self.cluster)
            for name, help_text, value in [
                ('discovery_write_success', 'Last target and state writes succeeded.', int(self.write_ok)),
                ('discovery_inventory_loaded', 'Independent operator inventory is configured.', int(self.inventory_loaded)),
                ('discovery_retention_seconds', 'Maximum age of retained targets in seconds.', self.retention),
            ]:
                emit(name, help_text, 'gauge', [(base, value)])
            emit('discovery_write_errors_total', 'Snapshot writes failed since process start.', 'counter', [(base, self.write_errors)])
            fields = [
                ('discovery_success', 'Latest component discovery succeeded.', lambda c, s: int(s.get('success', False))),
                ('discovery_last_success_timestamp_seconds', 'Last successful discovery Unix time; zero means never.', lambda c, s: s.get('last_success', 0)),
                ('discovery_stale', 'Latest discovery failed or snapshot expired.', lambda c, s: int(not s.get('success', False) or now-s.get('last_success', 0) > self.retention)),
                ('discovery_targets', 'Targets in the last published file.', lambda c, s: len(self.published[c])),
                ('expected_targets', 'Targets in independent expected inventory.', lambda c, s: sum(g['labels']['component'] == c for g in self.expected)),
            ]
            for name, help_text, fn in fields:
                emit(name, help_text, 'gauge', [(dict(base, component=c), fn(c, self.state.get(c, {}))) for c in COMMANDS])
            emit('discovery_errors_total', 'Failed attempts by bounded reason.', 'counter',
                 [(dict(base, component=c, reason=r), self.state.get(c, {}).get('errors', {}).get(r, 0)) for c in COMMANDS for r in REASONS])
            emit('expected_target_info', 'Operator expected target independent of discovery and availability.', 'gauge',
                 [(dict({k: v for k, v in g['labels'].items() if k != 'job'}, target=g['targets'][0]), 1) for g in self.expected])
        return '\n'.join(lines) + '\n'


def serve_metrics(discovery, address):
    host, port = address.rsplit(':', 1)
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != '/metrics':
                self.send_error(404)
                return
            body = discovery.metrics().encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer((host, int(port)), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='target.ini')
    parser.add_argument('--interval', type=float)
    parser.add_argument('--count', type=int)
    parser.add_argument('--show', action='store_true')
    parser.add_argument('--mdsaddr')
    parser.add_argument('--cluster')
    parser.add_argument('--metrics-listen')
    args = parser.parse_args()
    config = configparser.ConfigParser()
    if not config.read(args.config):
        parser.error('discovery config not found')
    paths = config['path']
    settings = config['discovery'] if config.has_section('discovery') else {}
    interval = args.interval if args.interval is not None else float(settings.get('interval', 60))
    if not math.isfinite(interval) or interval <= 0 or (args.count is not None and args.count <= 0):
        parser.error('interval and count must be positive')
    state_path = paths.get('state_path', paths['target_path'] + '.state.json')
    Path(state_path).parent.mkdir(parents=True, exist_ok=True)
    lock_file = open(state_path + '.lock', 'a')
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error('another discovery process owns this state file')
    discovery = Discovery(paths['target_path'], state_path,
                          cluster=args.cluster or settings.get('cluster', 'default'),
                          inventory_path=paths.get('inventory_path') or None,
                          retention_seconds=float(settings.get('retention_seconds', 3600)),
                          timeout=float(settings.get('timeout', 5)), tool=settings.get('tool', 'dingo'))
    listen = args.metrics_listen if args.metrics_listen is not None else settings.get('metrics_listen', '127.0.0.1:19100')
    server = serve_metrics(discovery, listen) if listen else None
    try:
        count, success = 0, True
        while args.count is None or count < args.count:
            success = discovery.refresh(args.mdsaddr)
            count += 1
            if args.show:
                print(discovery.target_path.read_text(), flush=True)
            if args.count is None or count < args.count:
                time.sleep(interval)
        return 0 if success else 1
    finally:
        if server:
            server.shutdown()
            server.server_close()
        lock_file.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    raise SystemExit(main())
