import yaml
from elasticsearch import Elasticsearch

from estools import Datacenter, ElasticsearchCluster

with open('/home/gehel/.cumin/config.yaml', 'r') as f:
    cumin_config = yaml.safe_load(f)

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

cluster = ElasticsearchCluster(es, dc_name='eqiad', script_node=None, cumin_config=None, node_suffix='eqiad.wmnet', icinga=None, sudo=True, dry_run=True)

cluster.force_allocation_of_all_replicas()

