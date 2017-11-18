from __future__ import print_function
from dateutil import parser
import logging
import yaml
from datetime import timedelta

from estools import Datacenter
from estools.utils import timed

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('estools.upgrade')
logging.getLogger('curator').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('elasticsearch').setLevel(logging.ERROR)
logging.getLogger('cumin').setLevel(logging.WARNING)


def upgrade_cluster(task, start_time, wait_for_relocations):
    with open('/home/gehel/.cumin/config.yaml', 'r') as f:
        cumin_config = yaml.safe_load(f)

    message = 'upgrading elasticsearch cluster - {task}'.format(task=task)

    dc = Datacenter(cumin_config, sudo=True, dry_run=False)

    cluster = dc.elasticsearch_cluster('test', 'local')

    while True:
        nodes = cluster.next_nodes(start_time, n=3)
        if not nodes:
            break

        timed(
            action=lambda: upgrade_nodes(cluster, nodes, message, wait_for_relocations),
            message='upgrade of {nodes}'.format(nodes=nodes))


def upgrade_nodes(cluster, nodes, message, wait_for_relocations):
    for node in nodes:
        logger.info('starting upgrade for %s', node)

    timed(action=lambda: cluster.wait_for_green(timeout=timedelta(minutes=90)), message='wait for green')

    for node in nodes:
        timed(action=node.run_puppet_agent, message='puppet run')
        node.schedule_downtime(duration=timedelta(minutes=30), message=message)
        node.disable_puppet(message)

    try:
        timed(action=lambda: cluster.stop_replication(wait_for_relocations), message='stop replication')
        timed(action=cluster.flush_markers, message='flush markers')

        for node in nodes:
            node.depool()
            node.stop_elasticsearch()
            timed(action=node.upgrade_elasticsearch, message='upgrade elasticsearch')
            timed(action=node.reboot, message='reboot')
            timed(action=node.wait_for_elasticsearch, message='wait for elasticsearch')
            node.pool()
            node.enable_puppet(message)
            logger.info('upgrade done for %s', node)

    finally:
        cluster.start_replication(wait=False)

    logger.info('waiting for cluster to stabilize before next node')
    timed(action=lambda: cluster.wait_for_green(timeout=timedelta(minutes=90)), message='wait for green')

    if wait_for_relocations:
        timed(action=lambda: cluster.wait_for_no_relocations(timeout=timedelta(minutes=20)), message='wait for no relocation')


if __name__ == '__main__':
    start_time = parser.parse('2017-11-16T09:11:00')
    upgrade_cluster('T178411', start_time, wait_for_relocations=False)
