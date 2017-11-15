from __future__ import print_function
import yaml
from datetime import timedelta

from estools import Datacenter


def upgrade_cluster(task):
    with open('/home/gehel/.cumin/config.yaml', 'r') as f:
        cumin_config = yaml.safe_load(f)

    message = 'upgrading elasticsearch cluster - {task}'.format(task=task)

    dc = Datacenter(cumin_config, dry_run=True)

    cluster = dc.elasticsearch_cluster('relforge', 'eqiad')

    while True:
        node = cluster.next_node(start_time)
        if node:
            upgrade_node(cluster, node, message)
        else:
            break


def upgrade_node(cluster, node, message):
    print('Waiting for elasticsearch to be green')
    cluster.wait_for_green()

    print('Run puppet on {node}'.format(node=node))
    node.run_puppet_agent()

    print('Stop replication')
    cluster.stop_replication()

    print('flush markers')
    cluster.flush_markers()

    print('Schedule downtime for {node}'.format(node=node))
    node.schedule_downtime()

    print('Disable puppet for {node}'.format(node=node))
    node.disable_puppet(message)
    node.depool()
    node.stop_elasticsearch()
    node.upgrade_elasticsearch()
    node.reboot()
    node.wait_for_elasticsearch()
    cluster.start_replication()
    node.pool()
    node.enable_puppet(message)
    cluster.wait_for_green(timeout=timedelta(minutes=90))
    cluster.wait_for_no_relocations(timeout=timedelta(minutes=20))


if __name__ == '__main__':
    upgrade_cluster('T123456')
