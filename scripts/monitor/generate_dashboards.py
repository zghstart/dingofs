#!/usr/bin/env python3
"""Generate seven P0 dashboards from reviewed, existing raw metric sources.

Every query keeps the metric's original event boundary. No synthetic buckets,
backend labels or zero-filled missing data are introduced.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SELECT = 'cluster=~"$cluster",instance=~"$instance"'
DS = {'type': 'prometheus', 'uid': '$datasource'}


def raw(name, extra=''):
    return f'{name}{{{SELECT}{","+extra if extra else ""}}}'


def rate(name, extra=''):
    return f'rate({raw(name, extra)}[$__rate_interval])'


def panel(title, expressions, unit='short', description='', kind='timeseries', legend='{{instance}}'):
    if isinstance(expressions, str):
        expressions = [(expressions, legend)]
    return dict(title=title, type=kind, description=description,
                datasource=DS, targets=[dict(expr=q, refId=chr(65+i), legendFormat=legend,
                                            **({'instant': True, 'format': 'table'} if kind == 'table' else {}))
                                       for i, (q, legend) in enumerate(expressions)],
                fieldConfig=dict(defaults=dict(unit=unit, noValue='N/A'), overrides=[]),
                options=dict(legend=dict(displayMode='table', placement='bottom')))


def finish(filename, uid, title, panels):
    intro = dict(type='text', title='P0 source and interpretation',
                 options=dict(mode='markdown', content=(
                     '**P0 / existing sources.** N/A means missing data or no traffic denominator. '
                     'Scrape reachability is collection health. Quotas are per-instance snapshots. '
                     'Cluster p99, business SLI and measured Data Flow require P1.')),
                 gridPos=dict(x=0, y=0, w=24, h=4), id=1)
    result = [intro]
    for i, p in enumerate(panels):
        p.update(id=i+2, gridPos=dict(x=(i % 2)*12, y=4+(i//2)*9, w=12, h=9))
        result.append(p)
    dashboard = dict(uid=uid, title=title, schemaVersion=39, version=1,
                     tags=['dingofs', 'observability', 'P0'], editable=True,
                     refresh='15s', timezone='browser', time=dict(**{'from': 'now-1h', 'to': 'now'}),
                     templating=dict(list=[
                         dict(name='datasource', label='Prometheus', type='datasource', query='prometheus',
                              current=dict(text='DingoFS Prometheus', value='dingofs-prometheus')),
                         dict(name='cluster', type='query', datasource=DS, query='label_values(up, cluster)',
                              refresh=1, multi=True, includeAll=True, allValue='.*'),
                         dict(name='instance', type='query', datasource=DS,
                              query='label_values(up{cluster=~"$cluster"}, instance)',
                              refresh=1, multi=True, includeAll=True, allValue='.*'),
                         dict(name='node', type='query', datasource=DS,
                              query='label_values(up{cluster=~"$cluster"}, node)',
                              refresh=1, multi=True, includeAll=True, allValue='.*'),
                     ]), panels=result)
    dashboard['links'] = [dict(title={'mds':'MDS'}.get(name,name.title()), type='link', url='/d/dingofs-p0-'+name,
                              includeVars=True, keepTime=True)
                          for name in ('overview','client','mds','cache','store','capacity','host')]
    (ROOT/'grafana/provisioning/dashboards'/filename).write_text(json.dumps(dashboard, indent=2)+'\n')


def main():
    read, write = rate('dingofs_vfs_read_bps_total_count'), rate('dingofs_vfs_write_bps_total_count')
    verified_fs_read = rate('dingofs_vfs_read_bps_total_count', 'fs_id!=""')
    success, errors = rate('dingofs_fuse_op_all_qps_total_count'), rate('dingofs_fuse_op_all_error_num')
    error_ratio = f'sum by (cluster) ({errors}) / (sum by (cluster) ({success}) + sum by (cluster) ({errors}))'
    finish('overview.json', 'dingofs-p0-overview', 'DingoFS / Overview', [
        panel('Scrape reachability', raw('up', 'job=~"client|mds|remotecache|dingofs-node"'), 'bool'),
        panel('Discovery freshness', 'time() - dingofs_discovery_last_success_timestamp_seconds{cluster=~"$cluster"}', 's', legend='{{component}}'),
        panel('Client successful read and write bytes', [(f'sum by (cluster) ({read})', 'read'), (f'sum by (cluster) ({write})', 'write')], 'Bps'),
        panel('Client completion error ratio', error_ratio, 'percentunit', 'All aggregate only; errors/(successful+failed completions). Expected business errors are not yet separated.', legend='{{cluster}}'),
        panel('Top 10 client read bytes', f'topk(10, sum by (cluster, instance) ({read}))', 'Bps'),
        panel('Top 10 verified FS read bytes', f'topk(10, sum by (cluster, fs_id) ({verified_fs_read}))', 'Bps', 'Only clients whose discovered FS identity is verified are included.', legend='FS {{fs_id}}'),
        panel('Top 10 node CPU utilization', 'topk(10, 1 - avg by (cluster, node) (rate(node_cpu_seconds_total{cluster=~"$cluster",mode="idle"}[$__rate_interval])))', 'percentunit', legend='{{node}}'),
        panel('Missing expected targets', 'dingofs:expected_target_missing{cluster=~"$cluster"}', kind='table'),
        panel('Configured target relationships', 'dingofs_expected_target_info{cluster=~"$cluster"}', description='Inventory table only: node/component/target/FS. These are configured identities, not measured client-to-cache traffic.', kind='table'),
    ])
    client = [
        panel('Successful read and write operations', [(rate('dingofs_vfs_read_qps_total_count'), '{{instance}} read'), (rate('dingofs_vfs_write_qps_total_count'), '{{instance}} write')], 'ops'),
        panel('Successful read and write bytes', [(read, '{{instance}} read'), (write, '{{instance}} write')], 'Bps'),
        panel('Successful read average latency', f'{rate("dingofs_fuse_op_read_lat_total_value")} / {rate("dingofs_fuse_op_read_qps_total_count")} / 1e6', 's', 'Session/VFS user-space successful completions only; not the full application syscall.'),
        panel('All operation inflight', raw('dingofs_fuse_op_all_inflight_num')),
        panel('Completion errors', errors, 'ops'),
        panel('Read backpressure and write pressure counters', [
            (rate('vfs_'+name+'_num'), '{{instance}} '+name)
            for name in ['read_backpressure', 'read_backpressure_timeout', 'write_pressure_event',
                         'write_pressure_coalesced_event', 'write_pressure_round',
                         'write_pressure_round_failure', 'write_pressure_submit_failure']],
            description='Cumulative pressure events; no window gauges are passed to rate.'),
    ]
    for name in ['inflight_warmup_tasks', 'inflight_warmup_blocks', 'warmup_event_queue_depth', 'warmup_resolve_queued_tasks', 'warmup_resolve_running_tasks', 'warmup_dispatch_waiting_tasks']:
        client.append(panel(name.replace('_', ' ').title(), raw('dingofs_vfs_'+name)))
    client.append(panel('Warmup callback completion', [(rate('dingofs_vfs_warmup_blocks_succeeded_total'), '{{instance}} callback OK/Exist'), (rate('dingofs_vfs_warmup_blocks_failed_total'), '{{instance}} failed')], 'ops', 'Callback success does not prove cached data is already readable.'))
    # LatencyRecorder averages are microseconds. Keep instance; no cluster p99.
    for name in ['resolve_latency', 'task_latency']:
        source = 'dingofs_vfs_warmup_'+name
        active = 'dingofs_vfs_warmup_'+name.replace('_latency', '_qps')
        client.append(panel('Warmup '+name.replace('_', ' '), '('+raw(source)+' / 1e6) and on (cluster, instance) ('+raw(active)+' > 0)', 's', 'Per-instance window mean, shown only with positive source QPS. Idle/low-volume windows are N/A.'))
    finish('p0-client.json', 'dingofs-p0-client', 'DingoFS / Client', client)
    finish('p0-mds.json', 'dingofs-p0-mds', 'DingoFS / MDS', [
        panel('Lookup framework QPS', f'{{__name__=~".*dingofs_pb_mds_mdsservice_lookup_qps",{SELECT}}}', 'ops', 'Framework transport results, not response.error business success.'),
        panel('WorkerSet pending tasks', f'{{__name__=~"dingo_worker_set_.*pending.*",{SELECT}}}', legend='{{instance}} {{__name__}}'),
        panel('Metadata cache hits and misses', [(f'rate({{__name__=~"dingofs_[0-9]+_(inode|partition|chunk)_cache_(hit|miss)_count",{SELECT}}}[$__rate_interval])', '{{instance}} fs={{fs_id}} {{cache}} {{event}}')], 'ops', 'Separate cache and FS identities. Insert totals are not entries.'),
        panel('MDS resident memory', raw('process_memory_resident', 'component="mds"'), 'bytes'),
    ])
    finish('p0-cache.json', 'dingofs-p0-cache', 'DingoFS / Cache', [
        panel('Remote tier conditional hit ratio', f'{rate("dingofs_remote_cache_hit_count")} / ({rate("dingofs_remote_cache_hit_count")} + {rate("dingofs_remote_cache_miss_count")})', 'percentunit', 'Same tier and time window. This is not the global origin miss ratio.'),
        panel('Disk used bytes', f'{{__name__=~"dingofs_disk_cache_[0-9]+_used_bytes",{SELECT}}}', 'bytes', legend='{{instance}} {{__name__}}'),
        panel('Disk full flags', f'{{__name__=~"dingofs_disk_cache_[0-9]+_(stage|cache)_full",{SELECT}}}', 'bool', legend='{{instance}} {{__name__}}'),
        panel('Disk health states', raw('dingofs_cache_health_state'), description='All fixed states are emitted as one-hot values. Missing or failed samples become unknown using disk identities retained for five minutes, then N/A.', legend='{{instance}} disk={{disk}} {{state}}'),
        panel('Disk running states', raw('dingofs_cache_running_state'), legend='{{instance}} disk={{disk}} {{state}}'),
        panel('Remote tier bytes from one canonical alias', rate('dingofs_remote_cache_cluster_range_total_bytes'), 'Bps', 'remote_node_group compatibility alias is excluded to prevent double counting.'),
    ])
    finish('store.json', 'dingofs-p0-store', 'DingoFS / Storage Backend', [
        panel('Successful read and write bytes', [(rate('dingofs_block_read_block_bps_total_count'), '{{instance}} read'), (rate('dingofs_block_write_block_bps_total_count'), '{{instance}} write')], 'Bps', 'Requires OBS-05 fixed binary. Successful actual returned/accepted bytes, wrapper boundary, no inferred backend identity.'),
        panel('Successful read and write requests', [(rate('dingofs_block_read_block_qps_total_count'), '{{instance}} read'), (rate('dingofs_block_write_block_qps_total_count'), '{{instance}} write')], 'ops'),
        panel('Read and write errors', [(rate('dingofs_block_read_block_eps_total_count'), '{{instance}} read'), (rate('dingofs_block_write_block_eps_total_count'), '{{instance}} write')], 'ops'),
        panel('Inflight PUTs', raw('block_put_async_num')+' + '+raw('block_put_sync_num'), description='Gauge: increment on start, decrement on completion. Never rate this value.'),
        panel('Inflight GETs', raw('block_get_async_num')+' + '+raw('block_get_sync_num')),
        panel('Successful average read latency', f'{rate("dingofs_block_read_block_lat_total_value")} / {rate("dingofs_block_read_block_qps_total_count")} / 1e6', 's'),
    ])
    finish('capacity.json', 'dingofs-p0-capacity', 'DingoFS / Capacity and Quota', [
        panel('Per-instance quota used bytes', f'{{__name__=~"fs_quota_.*_used_bytes",{SELECT},component="mds"}}', 'bytes', 'Replica snapshots: never sum replicas or claim owner freshness.', kind='table'),
        panel('Per-instance quota max bytes', f'{{__name__=~"fs_quota_.*_max_bytes",{SELECT},component="mds"}}', 'bytes', 'Zero/unlimited is not a finite quota; no utilization ratio is derived.', kind='table'),
        panel('Per-instance used inodes', f'{{__name__=~"fs_quota_.*_used_inodes",{SELECT},component="mds"}}', kind='table'),
        panel('Local cache physical capacity', f'{{__name__=~"dingofs_disk_cache_[0-9]+_capacity",{SELECT}}}', 'bytes', 'Cache physical space is distinct from logical FS quota and object storage capacity.', legend='{{instance}} {{__name__}}'),
    ])
    finish('host.json', 'dingofs-p0-host', 'DingoFS / Host and Runtime', [
        panel('Host CPU utilization', '1 - avg by (cluster, node) (rate(node_cpu_seconds_total{cluster=~"$cluster",node=~"$node",mode="idle"}[$__rate_interval]))', 'percentunit', legend='{{node}}'),
        panel('Host available memory', 'node_memory_MemAvailable_bytes{cluster=~"$cluster",node=~"$node"}', 'bytes', legend='{{node}}'),
        panel('Process resident memory', raw('process_memory_resident'), 'bytes'),
        panel('Process CPU cores', raw('process_cpu_usage'), 'short', 'CPU usage in cores, not percent of host capacity.'),
        panel('bthread worker utilization', raw('bthread_worker_usage')+' / '+raw('bthread_worker_count'), 'percentunit'),
        panel('Scrape duration', raw('scrape_duration_seconds'), 's'),
        panel('Scraped samples', raw('scrape_samples_scraped')),
        panel('Node scrape coverage', raw('up', 'job="dingofs-node"'), 'bool'),
    ])


if __name__ == '__main__':
    main()
