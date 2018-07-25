from __future__ import print_function

from dateutil import parser
import logging
import yaml
from datetime import timedelta
import time

from tqdm import tqdm

from estools import Datacenter, ElasticNodes, MaxWriteQueueExceeded
from estools.utils import TimeoutException, timer

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

        execute_on_nodes(cluster, message, nodes, task, wait_for_relocations)


def execute_on_nodes(cluster, message, nodes, task, wait_for_relocations):
    with timer('{message} on {nodes}'.format(message=message, nodes=nodes)):

        with timer('wait for green'):
            cluster.wait_for_green(timeout=timedelta(minutes=90))

        with nodes.puppet_disabled(message):
            nodes.schedule_downtime(duration=timedelta(minutes=30), message=message)

            with cluster.frozen_writes():
                logger.info('waiting for writes to settle')
                time.sleep(60)

                with cluster.stopped_replication(wait_for_relocations):
                    cluster.flush_markers()
                    nodes.depool()

                    task(nodes)

                    with timer('wait for elasticsearch'):
                        nodes.wait_for_elasticsearch()

                    nodes.pool()
                # replication enabled

                # time.sleep(60)
                # cluster.force_allocation_of_all_replicas()

                try:
                    with timer('wait for green'):
                        # wait 15' or until the write queue is too large to thaw writes
                        cluster.wait_for_green(timeout=timedelta(minutes=5))
                except MaxWriteQueueExceeded as e:
                    logger.warning(
                        'Write queue size has grown too large, thawing writes and continuing (delayed jobs: %d)',
                        e.queue_status['delayed'])
                except TimeoutException:
                    logger.warning('Timeout exceeded, thawing writes and continuing.')

            # writes thawed
        # puppet enabled

        logger.info('waiting for cluster to stabilize before next node')
        with timer('wait for green'):
            cluster.wait_for_green(timeout=timedelta(minutes=90))

        # with timer('wait for write queue to drain'):
        #     cluster.wait_for_write_queue_to_drain()

        if wait_for_relocations:
            with timer('wait for no relocation'):
                cluster.wait_for_no_relocations(timeout=timedelta(minutes=20))


def upgrade_nodes(nodes):
    logger.info('starting upgrade for %s', nodes)

    nodes.stop_elasticsearch()
    with timer('upgrade elasticsearch'):
        nodes.upgrade_elasticsearch()
    with timer('reboot'):
        nodes.reboot()
    logger.info('upgrade done for %s', nodes)


def upgrade_plugins(nodes):
    logger.info('starting plugin upgrade for %s', nodes)

    nodes.stop_elasticsearch()
    with timer('upgrade plugins'):
        nodes.upgrade_elasticsearch_plugins()

    nodes.start_elasticsearch()

    logger.info('plugin upgrade upgrade done for %s', nodes)


def restart_elasticsearch(nodes):
    logger.info('restarting %s', nodes)

    nodes.stop_elasticsearch()
    nodes.start_elasticsearch()
    logger.info('restart completed for %s', nodes)


def reboot_nodes(nodes):
    logger.info('starting reboot for %s', nodes)

    with timer('reboot'):
        nodes.reboot()


if __name__ == '__main__':
    start_time = parser.parse('2018-07-25T01:00:00')
    execute_on_cluster(
        message='restart for ',
        phab_number='T156137',
        start_time=start_time,
        wait_for_relocations=False,
        task=restart_elasticsearch,
        dry_run=False)
