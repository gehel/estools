from datetime import datetime, timedelta
import yaml
from cumin.transport import Transport
from cumin.transports import Target
from elasticsearch import Elasticsearch

from estools import Datacenter
from estools.should_be_externalized import Node

with open('/home/gehel/.cumin/config.yaml', 'r') as f:
    cumin_config = yaml.safe_load(f)
#
dc = Datacenter(cumin_config, sudo=True, dry_run=True)

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# cluster = ElasticsearchCluster(es, cumin_config=None, node_suffix='codfw.wmnet', dry_run=True)
cluster = dc.elasticsearch_cluster('test', 'local')

cluster.wait_for_green(timedelta(seconds=30))

# start_time = datetime.utcnow() - timedelta(days=3)
# node = cluster.next_node(restart_start_time=start_time)
#
# node = Node('elastic2029.codfw.wmnet', cumin_config, False, None)
# up = node.uptime()
#
# print up

# cumin_config = {
#     'backend': 'puppetdb',
#     'transport': 'clustershell'
# }
# worker = Transport.new(cumin_config, Target(['elastic2001.codfw.wmnet']))
# worker.commands = ['cat /etc/sudoers.d/ops']
# worker.handler = 'sync'
#
# worker.execute()
#
# message = None
# # we executed on a single node, there should be a single result
# for _, output in worker.get_results():
#     message = output.message()
#     print message
#
