from __future__ import print_function

from datetime import datetime, timedelta

import logging
import curator
from elasticsearch import Elasticsearch, TransportError, ConflictError

from estools.should_be_externalized import Node, Icinga, RemoteExecutionError
from estools.utils import wait_for

elasticsearch_clusters = {
    'search': {
        'eqiad': {
            'endpoint': 'search.svc.eqiad.wmnet:9200',
            'suffix': 'eqiad.wmnet',
        },
        'codfw': {
            'endpoint': 'search.svc.codfw.wmnet:9200',
            'suffix': 'codfw.wmnet',
        },
    },
    'relforge': {
        'eqiad': {
            'endpoint': 'relforge1002.eqiad.wmnet:9200',
            'suffix': 'eqiad.wmnet',
        },
    },
    'test': {
        'local': {
            'endpoint': 'localhost:9200',
            'suffix': 'eqiad.wmnet',
        },
    },
}


class Datacenter(object):
    def __init__(self, cumin_config, sudo, dry_run=False):
        self.cumin_config = cumin_config
        self.sudo = sudo
        self.dry_run = dry_run

    def icinga(self):
        return Icinga('einsteinium.wikimedia.org', self.cumin_config, self.sudo, self.dry_run)

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
            return ElasticsearchCluster(Elasticsearch(endpoint), self.cumin_config, suffix, self.icinga(), self.sudo, self.dry_run)
        except KeyError:
            raise ConfigError('No cluster named {name} exist in DC {site}'.format(name=name, site=site))


class ElasticsearchCluster(object):
    def __init__(self, elasticsearch, cumin_config, node_suffix, icinga, sudo, dry_run=False):
        self.elasticsearch = elasticsearch
        self.cumin_config = cumin_config
        self.node_suffix = node_suffix
        self.icinga = icinga
        self.sudo = sudo
        self.dry_run = dry_run
        self.logger = logging.getLogger('estools.cluster')

    def stop_replication(self, wait=True):
        self.logger.info('stop replication')
        self._do_cluster_routing(
            curator.ClusterRouting(self.elasticsearch, routing_type='allocation', setting='enable',
                                   value='primaries', wait_for_completion=wait)
        )

    def start_replication(self, wait=True):
        self.logger.info('start replication')
        self._do_cluster_routing(
            curator.ClusterRouting(self.elasticsearch, routing_type='allocation', setting='enable',
                                   value='all', wait_for_completion=wait)
        )

    def wait_for_green(self, timeout=timedelta(hours=1)):
        self.logger.info('waiting for cluster to be green')

        def green():
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

    def flush_markers(self):
        self.logger.info('flush markers')
        if not self.dry_run:
            try:
                self.elasticsearch.indices.flush_synced()
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

        def compare_row_size((_, val1), (__, val2)):
            return len(val1['done']) - len(val2['done'])

        s = sorted(rows.items(), cmp=compare_row_size)
        for row_name, row in s:
            if len(row['not_done']) > 0:
                return [self._new_node(node['name']) for node in row['not_done'][:n]]
        return []

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

    def _new_node(self, hostname):
        fqdn = hostname + '.' + self.node_suffix
        return ElasticNode(fqdn, self.cumin_config, self.dry_run, self.icinga, self.sudo)


class ElasticNode(Node):

    def __init__(self, fqdn, cumin_config, dry_run, icinga, sudo):
        super(ElasticNode, self).__init__(fqdn, cumin_config, dry_run, icinga, sudo)

    def stop_elasticsearch(self):
        self.stop_service('elasticsearch')

    def wait_for_elasticsearch(self):
        self.logger.info('waiting for elasticsearch to bu up on %s', self)
        wait_for(
            lambda: self.is_elasticsearch_up(),
            timeout=timedelta(seconds=300),
            ignored_exceptions=[RemoteExecutionError]
        )

    def is_elasticsearch_up(self):
        rc, message = self.execute('curl -s 127.0.0.1:9200/_cat/health', safe=True)
        if rc != 0:
            self.logger.info('elasticsearch not yet up: %s', message)
        return rc == 0

    def upgrade_elasticsearch(self):
        self.upgrade_packages(['elasticsearch', 'wmf-elasticsearch-search-plugins'])


class ConfigError(Exception):
    pass

