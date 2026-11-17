"""Failure-oriented tests for the real target-discovery implementation."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.request import urlopen
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('target_json', Path(__file__).resolve().parents[1] / 'target_json.py')
m = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m)


def response(**result):
    return subprocess.CompletedProcess([], 0, json.dumps({'error': {'code': 0}, 'result': dict(error={}, **result)}))


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.now = 1000
        self.outputs = {
            'mds': response(mdses=[{'location': {'host': '10.0.0.1', 'port': 7801}}]),
            'client': response(fs_infos=[{'fs_id': 42, 'mount_points': [
                {'ip': '10.0.0.2', 'port': 10000}, {'ip': '10.0.0.2', 'port': 10001},
                {'ip': '10.0.0.2', 'port': 10000}]}]),
            'cache': response(members=[{'ip': '10.0.0.3', 'port': 9300}]),
        }
        self.inventory = self.root / 'expected.json'
        self.inventory.write_text(json.dumps([{'targets': ['10.0.0.1:7801'], 'labels': {
            'component': 'mds', 'cluster': 'test', 'node': 'meta-1'}}]))
        self.discovery = self.make()

    def runner(self, command, **kwargs):
        component = next(c for c, prefix in m.COMMANDS.items() if command[1:1+len(prefix)] == prefix)
        value = self.outputs[component]
        if isinstance(value, Exception):
            raise value
        return value

    def make(self):
        return m.Discovery(self.root/'targets.json', self.root/'state.json', 'test', self.inventory,
                           retention_seconds=100, runner=self.runner, clock=lambda: self.now)

    def targets(self):
        return json.loads((self.root/'targets.json').read_text())

    def test_dedup_identity_and_multiple_mounts(self):
        self.assertTrue(self.discovery.refresh())
        self.assertEqual(len(self.targets()), 4)
        clients = [g for g in self.targets() if g['labels']['component'] == 'client']
        self.assertEqual(len(clients), 2)
        self.assertEqual({g['labels']['fs_id'] for g in clients}, {'42'})
        mds = next(g for g in self.targets() if g['labels']['component'] == 'mds')
        self.assertEqual(mds['labels']['node'], 'meta-1')
        self.assertEqual((self.root/'targets.json').stat().st_mode & 0o777, 0o644)

    def test_each_failure_retains_only_its_last_success(self):
        failures = [subprocess.CompletedProcess([], 1, '{}'), subprocess.TimeoutExpired([], 1),
                    subprocess.CompletedProcess([], 0, ''), subprocess.CompletedProcess([], 0, '{'),
                    subprocess.CompletedProcess([], 0, '{"result":{}}'),
                    response(mdses={}), response(mdses=[{}]),
                    subprocess.CompletedProcess([], 0, '{"error":{"code":12},"result":{}}')]
        for failure in failures:
            with self.subTest(failure=failure):
                self.outputs['mds'] = response(mdses=[{'location': {'host': '10.0.0.1', 'port': 7801}}])
                self.assertTrue(self.discovery.refresh())
                old = self.targets()
                self.now += 1
                self.outputs['mds'] = failure
                self.assertFalse(self.discovery.refresh())
                self.assertEqual(self.targets(), old)
                self.assertEqual(self.discovery.state['mds']['last_success'], self.now-1)
                self.assertTrue(self.discovery.state['client']['success'])
                self.assertFalse(self.discovery.state['mds']['success'])

    def test_empty_success_removes_target_but_inventory_survives(self):
        self.discovery.refresh()
        self.outputs['mds'] = response(mdses=[])
        self.assertTrue(self.discovery.refresh())
        self.assertFalse(any(g['labels']['component'] == 'mds' for g in self.targets()))
        self.assertIn('target="10.0.0.1:7801"', self.discovery.metrics())
        self.outputs['mds'] = response()  # protobuf omits empty repeated fields
        self.assertTrue(self.discovery.refresh())
        self.outputs['mds'] = subprocess.CompletedProcess([], 0, json.dumps({
            'error': {'code': 0}, 'result': {'info': {'time': {'total_time_us': 12}}}}))
        self.assertTrue(self.discovery.refresh())
        self.outputs['mds'] = subprocess.CompletedProcess([], 0, '{"error":{"code":0},"result":{}}')
        self.assertTrue(self.discovery.refresh())
        self.outputs['mds'] = response(unexpected=[])
        self.assertFalse(self.discovery.refresh())

    def test_restart_preserves_error_and_timestamp_then_expires(self):
        self.discovery.refresh()
        self.outputs['mds'] = subprocess.CompletedProcess([], 1, '')
        self.now = 1050
        restarted = self.make()
        self.assertFalse(restarted.state['mds']['success'])
        self.assertFalse(restarted.refresh())
        self.assertTrue(any(g['labels']['component'] == 'mds' for g in self.targets()))
        self.now = 1101
        restarted.refresh()
        self.assertFalse(any(g['labels']['component'] == 'mds' for g in self.targets()))
        self.assertIn('target="10.0.0.1:7801"', restarted.metrics())
        self.assertEqual(restarted.state['mds']['errors']['command'], 2)

    def test_failure_before_any_success_and_valid_empty_inventory(self):
        for c in self.outputs:
            self.outputs[c] = FileNotFoundError()
        self.assertFalse(self.discovery.refresh())
        self.assertEqual(self.targets(), [])
        self.assertEqual(self.discovery.state['mds']['last_success'], 0)
        self.inventory.write_text('[]')
        self.assertIn('dingofs_discovery_inventory_loaded{cluster="test"} 1', self.make().metrics())

    def test_invalid_schema_cannot_erase_last_success(self):
        self.discovery.refresh()
        old = self.targets()
        for value in [None, [None], [{'fs_id': 42, 'mount_points': [{}]}],
                      [{'fs_id': 42, 'mount_points': 'wrong'}],
                      [{'fs_id': 42, 'mount_points': [{'ip': '10.0.0.2', 'port': True}]}]]:
            self.outputs['client'] = response(fs_infos=value)
            self.assertFalse(self.discovery.refresh())
            self.assertEqual(self.targets(), old)

    def test_conflicting_fs_identity_is_a_failure(self):
        self.discovery.refresh()
        rows = [{'fs_id': i, 'mount_points': [{'ip': '10.0.0.2', 'port': 10000}]} for i in (42, 43)]
        self.outputs['client'] = response(fs_infos=rows)
        self.assertFalse(self.discovery.refresh())
        self.assertEqual(len(self.targets()), 4)

    def test_atomic_write_failure_preserves_published_targets(self):
        self.discovery.refresh()
        old = (self.root/'targets.json').read_bytes()
        self.outputs['mds'] = response(mdses=[])
        with patch.object(m.os, 'replace', side_effect=OSError('disk full')):
            self.assertFalse(self.discovery.refresh())
        self.assertEqual((self.root/'targets.json').read_bytes(), old)
        self.assertFalse(self.discovery.write_ok)
        self.assertEqual(self.discovery.write_errors, 1)

    def test_bad_state_fails_closed_and_legacy_targets_are_unverified(self):
        self.discovery.refresh()
        (self.root/'state.json').write_text('{}')
        with self.assertRaises(ValueError):
            self.make()
        (self.root/'state.json').unlink()
        migrated = self.make()
        self.assertEqual(migrated.state['mds']['last_success'], 0)
        self.outputs['mds'] = FileNotFoundError()
        migrated.refresh()
        self.assertTrue(any(g['labels']['component'] == 'mds' for g in self.targets()))

    def test_ipv6_and_invalid_addresses(self):
        self.assertEqual(m.endpoint('2001:db8::1', 9300), '[2001:db8::1]:9300')
        for host, port in [('bad host', 9300), ('host/path', 9300), ('host', 0), ('host', True)]:
            with self.assertRaises(ValueError):
                m.endpoint(host, port)

    def test_metrics_endpoint_content_type_and_current_staleness(self):
        self.discovery.refresh()
        server = m.serve_metrics(self.discovery, '127.0.0.1:0')
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        with urlopen(f'http://127.0.0.1:{server.server_port}/metrics') as result:
            self.assertIn('version=0.0.4', result.headers['Content-Type'])
            self.assertIn('# TYPE dingofs_discovery_errors_total counter', result.read().decode())
        self.now += 101
        self.assertIn('dingofs_discovery_stale{cluster="test",component="mds"} 1', self.discovery.metrics())

    def test_cli_rejects_a_second_writer_for_the_same_state(self):
        tool = self.root/'dingo'
        tool.write_text('#!/bin/sh\nprintf \'%s\\n\' \'{"error":{"code":0},"result":{"error":{}}}\'\n')
        tool.chmod(0o755)
        config = self.root/'target.ini'
        config.write_text(f'[path]\ntarget_path={self.root}/targets.json\nstate_path={self.root}/state.json\n'
                          f'[discovery]\ncluster=test\ntool={tool}\n')
        command = [sys.executable, str(Path(m.__file__)), '--config', str(config), '--metrics-listen', '']
        first = subprocess.Popen(command+['--interval','0.05'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(100):
                if (self.root/'state.json').exists():
                    break
                time.sleep(0.05)
            self.assertIsNone(first.poll())
            self.assertTrue((self.root/'state.json').exists())
            second = subprocess.run(command+['--count','1'], capture_output=True, text=True, timeout=5)
            self.assertNotEqual(second.returncode, 0)
            self.assertIn('another discovery process owns this state file', second.stderr)
        finally:
            first.terminate()
            first.wait(timeout=5)


if __name__ == '__main__':
    unittest.main()
