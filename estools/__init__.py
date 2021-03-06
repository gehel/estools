from __future__ import print_function

import re
from contextlib import contextmanager
from datetime import datetime, timedelta

import logging
from random import shuffle

import curator
from elasticsearch import Elasticsearch, TransportError, ConflictError, RequestError

from estools.should_be_externalized import Nodes, Icinga, RemoteExecutionError
from estools.utils import wait_for

elasticsearch_clusters = {
    'search': {
        'eqiad': {
            'endpoint': 'search.svc.eqiad.wmnet:9200',
            'suffix': 'eqiad.wmnet',
            'dc_name': 'eqiad',
        },
        'codfw': {
            'endpoint': 'search.svc.codfw.wmnet:9200',
            'suffix': 'codfw.wmnet',
            'dc_name': 'codfw',
        },
    },
    'relforge': {
        'eqiad': {
            'endpoint': 'relforge1002.eqiad.wmnet:9200',
            'suffix': 'eqiad.wmnet',
            'dc_name': 'eqiad',
        },
    },
    'test': {
        'local': {
            'endpoint': 'localhost:9200',
            'suffix': 'codfw.wmnet',
            'dc_name': 'codfw',
        },
    },
}


class Datacenter(object):
    def __init__(self, cumin_config, sudo, dry_run=False):
        self.cumin_config = cumin_config
        self.sudo = sudo
        self.dry_run = dry_run

    def icinga(self):
        return Icinga('icinga.wikimedia.org', self.cumin_config, self.sudo, self.dry_run)

    def script_node(self):
        return ScriptNode('terbium.eqiad.wmnet', self.cumin_config, self.dry_run, self.icinga())

    def elasticsearch_cluster(self, name, site):
        """Create an ElasticsearchCluster object for the given cluster / DC
        :param name: name of the cluster (search, relforge, ...)
        :param site: site in which the cluster is (eqiad, codfw, ...)

        >>> dc = Datacenter(cumin_config={})
        >>> c = dc.elasticsearch_cluster('search', 'eqiad')
        >>> c.elasticsearch.transport.hosts
        [{u'host': u'search.svc.eqiad.wmnet', u'port': 9200}]
        >>> c = dc.elasticsearch_cluster('test', 'local')
        >>> c.elasticsearch.transport.hosts
        [{u'host': u'localhost', u'port': 9200}]
        >>> c = dc.elasticsearch_cluster('search', 'non-existing-site')
        Traceback (most recent call last):
          ...
        ConfigError: No cluster named search exist in DC non-existing-site
        >>> c = dc.elasticsearch_cluster('non-existing-cluster', 'eqiad')
        Traceback (most recent call last):
          ...
        ConfigError: No cluster named non-existing-cluster exist in DC eqiad
        """
        try:
            endpoint = elasticsearch_clusters[name][site]['endpoint']
            suffix = elasticsearch_clusters[name][site]['suffix']
            dc_name = elasticsearch_clusters[name][site]['dc_name']
            return ElasticsearchCluster(Elasticsearch(endpoint), dc_name, self.script_node(), self.cumin_config, suffix, self.icinga(), self.sudo, self.dry_run)
        except KeyError:
            raise ConfigError('No cluster named {name} exist in DC {site}'.format(name=name, site=site))


class ElasticsearchCluster(object):
    def __init__(self, elasticsearch, dc_name, script_node, cumin_config, node_suffix, icinga, sudo, dry_run=False):
        self.elasticsearch = elasticsearch
        self.dc_name = dc_name
        self.script_node = script_node
        self.cumin_config = cumin_config
        self.node_suffix = node_suffix
        self.icinga = icinga
        self.sudo = sudo
        self.dry_run = dry_run
        self.logger = logging.getLogger('estools.cluster')

    @contextmanager
    def frozen_writes(self):
        self._freeze_writes()
        try:
            yield
        finally:
            self._thaw_writes()

    def _freeze_writes(self):
        self.script_node.mwscript(
            'extensions/CirrusSearch/maintenance/freezeWritesToCluster.php',
            [
                '--wiki=enwiki',
                '--cluster={cluster}'.format(cluster=self.dc_name)
            ]
        )

    def _thaw_writes(self):
        self.script_node.mwscript(
            'extensions/CirrusSearch/maintenance/freezeWritesToCluster.php',
            [
                '--wiki=enwiki',
                '--cluster={cluster}'.format(cluster=self.dc_name),
                '--thaw'
            ]
        )

    def write_queue_status(self):
        _, message = self.script_node.mwscript('showJobs.php', ['--wiki=enwiki',  '--group'], safe=True)
        match = re.search(
            r'^cirrusSearchElasticaWrite: (?P<queued>\d+) queued; (?P<claimed>\d+) claimed \((?P<active>\d+) active, (?P<abandoned>\d+) abandoned\); (?P<delayed>\d+) delayed$',
            message, flags=re.M)
        if match:
            return {
                'queued': int(match.group('queued')),
                'claimed': int(match.group('claimed')),
                'active': int(match.group('active')),
                'abandoned': int(match.group('abandoned')),
                'delayed': int(match.group('delayed'))
            }
        else:
            return {}

    @contextmanager
    def stopped_replication(self, wait=True):
        self._stop_replication(wait)
        try:
            yield
        finally:
            self._start_replication(wait)

    def _stop_replication(self, wait=True):
        self.logger.info('stop replication')
        self._do_cluster_routing(
            curator.ClusterRouting(self.elasticsearch, routing_type='allocation', setting='enable',
                                   value='primaries', wait_for_completion=wait)
        )

    def _start_replication(self, wait=True):
        self.logger.info('start replication')
        self._do_cluster_routing(
            curator.ClusterRouting(self.elasticsearch, routing_type='allocation', setting='enable',
                                   value='all', wait_for_completion=wait)
        )

    def wait_for_green(self, timeout=timedelta(hours=1), max_delayed_jobs=None):
        self.logger.info('waiting for cluster to be green')

        def green():
            if max_delayed_jobs:
                status = self.write_queue_status()
                if status and status['delayed'] > max_delayed_jobs:
                    raise MaxWriteQueueExceeded(status)
            return self.elasticsearch.cluster.health(wait_for_status='green', params={'timeout': '1s'})
        wait_for(green, retry_period=timedelta(seconds=10), timeout=timeout, ignored_exceptions=[TransportError])
        self.logger.info('cluster is green')

    def wait_for_no_relocations(self, timeout=timedelta(minutes=20)):
        self.logger.info('waiting for relocations to stabilize')

        def no_relocations():
            relocations = self.elasticsearch.indices.recovery(active_only=True)
            return len(relocations) == 0
        wait_for(no_relocations, retry_period=timedelta(seconds=10), timeout=timeout, ignored_exceptions=[TransportError])
        self.logger.info('no more relocations in progress')

    def wait_for_write_queue_to_drain(self, timeout=timedelta(minutes=20)):
        self.logger.info('waiting for relocations to stabilize')

        def drained():
            status = self.write_queue_status()
            return status == {} or status['delayed'] == 0
        wait_for(drained, retry_period=timedelta(seconds=10), timeout=timeout)

    def flush_markers(self):
        self.logger.info('flush markers')
        if not self.dry_run:
            try:
                self.elasticsearch.indices.flush(force=True, request_timeout=60)
            except ConflictError:
                self.logger.exception('Not all shards have been flushed, which should not be an issue.')
            try:
                self.elasticsearch.indices.flush_synced(request_timeout=60)
            except ConflictError:
                self.logger.exception('Not all shards have been flushed, which should not be an issue.')

    def _do_cluster_routing(self, cluster_routing):
        if self.dry_run:
            cluster_routing.do_dry_run()
        else:
            cluster_routing.do_action()

    def next_nodes(self, restart_start_time, n=1):
        info = self.elasticsearch.nodes.info()
        rows = self._to_rows(info['nodes'], restart_start_time)

        s = sorted(rows.items(), key=lambda row: len(row[1]['done']))
        for row_name, row in s:
            if len(row['not_done']) > 0:
                nodes_names = [node['name'] + '.' + self.node_suffix for node in row['not_done'][:n]]
                return ElasticNodes(nodes_names, self.cumin_config, self.dry_run, self.icinga, self.sudo)
        return None

    def _to_rows(self, nodes, start_time):
        rows = {}
        for _, node in nodes.items():
            row = node['attributes']['row']
            self._ensure_row_initialized(row, rows)

            if self._has_been_restarted_after(node, start_time):
                rows[row]['done'].append(node)
            else:
                rows[row]['not_done'].append(node)
        return rows

    def _ensure_row_initialized(self, row, rows):
        if row not in rows:
            rows[row] = {'done': [], 'not_done': []}

    def _has_been_restarted_after(self, node, start_time):
        jvm_start = datetime.utcfromtimestamp(int(node['jvm']['start_time_in_millis'] / 1000))
        b = jvm_start > start_time
        return b

    def force_allocation_of_all_replicas(self):
        max = len(self.unassigned_shards())
        i = 0
        while i < max:
            unassigned = self.unassigned_shards()
            if len(unassigned) == 0:
                # no more unassigned shards, we're done
                return
            self.force_allocation_of_shard(unassigned[0])
            i = i + 1

    def unassigned_shards(self):
        shards = self.elasticsearch.cat.shards(format='json', h='index,shard,state')
        return [s for s in shards if s['state'] == 'UNASSIGNED']

    def force_allocation_of_shard(self, shard):
        if self.is_shard_assigned(shard):
            self.logger.debug('Shard [%s:%s] is already assigned', shard['index'], shard['shard'])
            return
        es_nodes = self.elasticsearch.cat.nodes(h='name').splitlines()
        # shuffle nodes so that we don't allocate all shards on the same node
        shuffle(es_nodes)
        for node in es_nodes:
            try:
                self.logger.info('Trying to allocate [%s:%s] on [%s]', shard['index'], shard['shard'], node)
                self.elasticsearch.cluster.reroute(retry_failed=True, body={
                    'commands': [{
                        'allocate_replica': {
                            'index': shard['index'], 'shard': shard['shard'],
                            'node': node
                        }
                    }]
                })
                # successful allocation, we can exit
                self.logger.info('allocation successful')
                return
            except RequestError:
                # error allocating shard, let's try the next node
                pass
        self.logger.warning('Could not reallocate shard [%s:%s]', shard['index'], shard['shard'])

    def is_shard_assigned(self, shard):
        shards = self.elasticsearch.cat.shards(index=shard['index'], h='sh,st')
        match = re.search('{shard}\s*UNASSIGNED'.format(shard=shard['shard']), shards)
        return match is None


class ElasticNodes(Nodes):

    def __init__(self, fqdns, cumin_config, dry_run, icinga, sudo):
        super(ElasticNodes, self).__init__(fqdns, cumin_config, dry_run, icinga, sudo)

    def stop_elasticsearch(self):
        self.stop_service('elasticsearch')

    def start_elasticsearch(self):
        self.start_service('elasticsearch')

    def wait_for_elasticsearch(self):
        self.logger.info('waiting for elasticsearch to be up on %s', self)
        wait_for(
            lambda: self.is_elasticsearch_up(),
            timeout=timedelta(seconds=300),
            ignored_exceptions=[RemoteExecutionError]
        )

    def is_elasticsearch_up(self):
        rc, _ = self.execute('curl -s 127.0.0.1:9200/_cat/health', safe=True)
        if rc != 0:
            self.logger.info('elasticsearch not yet up on all nodes')
        return rc == 0

    def upgrade_elasticsearch(self):
        self.upgrade_packages(['elasticsearch', 'wmf-elasticsearch-search-plugins'])

    def upgrade_elasticsearch_plugins(self):
        self.upgrade_packages(['wmf-elasticsearch-search-plugins'])


class ScriptNode(Nodes):

    def __init__(self, fqdn, cumin_config, dry_run, icinga):
        super(ScriptNode, self).__init__([fqdn], cumin_config, dry_run, icinga, sudo=False)

    def mwscript(self, script, args, safe=False):
        args_string = ' '.join(args)
        return self.execute_single(
            'mwscript {script} {args}'.format(script=script, args=args_string),
            safe
        )


class ConfigError(Exception):
    pass


class MaxWriteQueueExceeded(Exception):

    def __init__(self, queue_status, *args):
        super(MaxWriteQueueExceeded, self).__init__(*args)
        self.queue_status = queue_status
