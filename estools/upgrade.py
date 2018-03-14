from __future__ import print_function

from dateutil import parser
import logging
import yaml
from datetime import timedelta
import time

from tqdm import tqdm

from estools import Datacenter, ElasticNodes, MaxWriteQueueExceeded
from estools.utils import timed, TimeoutException

logging.basicConfig(level=logging.DEBUG, stream=tqdm)

logging.getLogger()

logger = logging.getLogger('estools.upgrade')
logging.getLogger('curator').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('elasticsearch').setLevel(logging.ERROR)
logging.getLogger('cumin').setLevel(logging.WARNING)


def execute_on_cluster(message, phab_number, start_time, wait_for_relocations, task, parallelism=3, dry_run=True):
    with open('/home/gehel/.cumin/config.yaml', 'r') as f:
        cumin_config = yaml.safe_load(f)

    formatted_message = '{message} - {phab_number}'.format(message=message, phab_number=phab_number)

    dc = Datacenter(cumin_config, sudo=True, dry_run=dry_run)

    cluster = dc.elasticsearch_cluster('test', 'local')

    while True:
        nodes = cluster.next_nodes(start_time, n=parallelism)
        if not nodes:
            break

        timed(
            action=lambda: task(cluster, nodes, formatted_message, wait_for_relocations),
            message='{message} of {nodes}'.format(message=message, nodes=nodes))


def upgrade_nodes(cluster, nodes, message, wait_for_relocations):
    logger.info('starting upgrade for %s', nodes)

    timed(action=lambda: cluster.wait_for_green(timeout=timedelta(minutes=90)), message='wait for green')

    timed(action=nodes.run_puppet_agent, message='puppet run')
    nodes.schedule_downtime(duration=timedelta(minutes=30), message=message)
    nodes.disable_puppet(message)

    try:
        timed(action=lambda: cluster.stop_replication(wait_for_relocations), message='stop replication')
        timed(action=cluster.flush_markers, message='flush markers')

        nodes.depool()
        nodes.stop_elasticsearch()
        timed(action=nodes.upgrade_elasticsearch, message='upgrade elasticsearch')
        timed(action=nodes.reboot, message='reboot')
        timed(action=nodes.wait_for_elasticsearch, message='wait for elasticsearch')
        nodes.pool()
        nodes.enable_puppet(message)
        logger.info('upgrade done for %s', nodes)

    finally:
        cluster.start_replication(wait=False)

    logger.info('waiting for cluster to stabilize before next node')
    timed(action=lambda: cluster.wait_for_green(timeout=timedelta(minutes=90)), message='wait for green')

    if wait_for_relocations:
        timed(action=lambda: cluster.wait_for_no_relocations(timeout=timedelta(minutes=20)), message='wait for no relocation')


def reboot_nodes(cluster, nodes, message, wait_for_relocations):
    logger.info('starting reboot for %s', nodes)

    timed(action=lambda: cluster.wait_for_green(timeout=timedelta(minutes=90)), message='wait for green')

    with cluster.frozen_writes():

        logger.info('waiting for writes to settle')
        time.sleep(60)

        nodes.schedule_downtime(duration=timedelta(minutes=30), message=message)

        with cluster.stopped_replication(wait=False):
            timed(action=cluster.flush_markers, message='flush markers')

            nodes.depool()
            timed(action=nodes.reboot, message='reboot')
            timed(action=nodes.wait_for_elasticsearch, message='wait for elasticsearch')
            nodes.pool()
            logger.info('reboot done for %s', nodes)

        time.sleep(120)
        cluster.force_allocation_of_all_replicas()

        try:
            timed(action=lambda: cluster.wait_for_green(timeout=timedelta(minutes=15), max_delayed_jobs=15000), message='wait for green')
        except MaxWriteQueueExceeded as e:
            logger.warn('Write queue size has grown too large, unfreezing writes (delayed jobs: %d)', e.queue_status['delayed'])
        except TimeoutException:
            logger.warn('Timeout exceeded, thawing writes and continuing.')

    logger.info('waiting for cluster to stabilize before next nodes')
    timed(action=lambda: cluster.wait_for_green(timeout=timedelta(minutes=90)), message='wait for green')

    timed(action=cluster.wait_for_write_queue_to_drain, message="wait for write queue to drain")

    if wait_for_relocations:
        timed(action=lambda: cluster.wait_for_no_relocations(timeout=timedelta(minutes=20)), message='wait for no relocation')


if __name__ == '__main__':
    start_time = parser.parse('2018-03-01T06:00:00')
    execute_on_cluster(
        message='rebooting elasticsearch cluster',
        phab_number=None,
        start_time=start_time,
        wait_for_relocations=False,
        task=reboot_nodes,
        dry_run=False)

    # with open('/home/gehel/.cumin/config.yaml', 'r') as f:
    #     cumin_config = yaml.safe_load(f)
    # dc = Datacenter(cumin_config, sudo=True, dry_run=True)
    # cluster = dc.elasticsearch_cluster('test', 'local')
    # status = cluster.write_queue_status()
    # print(status)
    # fqdns = ['elastic1030.eqiad.wmnet', 'elastic10332.eqiad.wmnet', 'elastic1035.eqiad.wmnet']
    # nodes = ElasticNodes(fqdns, cumin_config, dry_run=False, icinga=dc.icinga(), sudo=True)
    # nodes.wait_for_reboot(start_time)
