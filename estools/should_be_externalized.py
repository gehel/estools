import logging

from cumin.transport import Transport
from datetime import timedelta, datetime

from cumin.transports import Target

from estools.utils import wait_for


class RemoteExecutionError(Exception):
    pass


class Node(object):

    def __init__(self, fqdn, cumin_config, dry_run, icinga, sudo):
        self.fqdn = fqdn
        self.cumin_config = cumin_config
        self.dry_run = dry_run
        self.icinga = icinga
        self.sudo = sudo
        self.logger = logging.getLogger('estools.node')
        self.hostname = fqdn.split('.')[0]

    def __repr__(self):
        return 'Node({node})'.format(node=self.fqdn)

    def run_puppet_agent(self):
        self.logger.info('run puppet on %s', self)
        self.execute('run-puppet-agent', safe=False)

    def schedule_downtime(self, duration, message):
        self.icinga.downtime(self, duration, message)

    def disable_puppet(self, message):
        self.logger.info('disable puppet on %s', self)
        self.execute('disable-puppet "{message}"'.format(message=message), safe=False)
        if not self.dry_run and self.is_puppet_enabled():
            raise RemoteExecutionError('Puppet still enabled.')

    def enable_puppet(self, message):
        self.logger.info('enable puppet on %s', self)
        self.execute('enable-puppet "{message}"'.format(message=message), safe=False)
        if not self.dry_run and not self.is_puppet_enabled():
            raise RemoteExecutionError('Puppet still disabled.')

    def is_puppet_enabled(self):
        rc, message = self.execute('puppet-enabled', safe=True)
        return rc == 0

    def depool(self):
        self.logger.info('depool %s', self)
        # ugly hack for sudo (which is already an ugly hack)
        if self.sudo:
            self.execute('-i depool', safe=False)
        else:
            self.execute('depool', safe=False)

    def pool(self):
        self.logger.info('pool %s', self)
        # ugly hack for sudo (which is already an ugly hack)
        if self.sudo:
            self.execute('-i pool', safe=False)
        else:
            self.execute('pool', safe=False)

    def stop_service(self, name):
        self.logger.info('stop service [%s] on %s', name, self)
        self.execute('service {name} stop'.format(name=name), safe=False)
        if self.is_service_running(name):
            raise RemoteExecutionError('Service {name} is still running.'.format(name=name))

    def start_service(self, name):
        self.logger.info('start service [%s] on %s', name, self)
        self.execute('service {name} start'.format(name=name), safe=False)
        if not self.is_service_running(name):
            raise RemoteExecutionError('Service {name} is still stopped.'.format(name=name))

    def is_service_running(self, name):
        rc, message = self.execute('service {name} status'.format(name=name), safe=False)
        if rc == 0:
            self.logger.info('service [%s] is running on %s', name, self)
        else:
            self.logger.info('service [%s] is NOT running on %s', name, self)
        return rc == 0

    def upgrade_packages(self, packages):
        self.logger.info('upgrade packages [%s] on %s', packages, self)
        self.execute(
            'apt-get {options} install {packages}'.format(
                options='-o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold"',
                packages=' '.join(packages)
            ), safe=False)

    def uptime(self):
        _, message = self.execute('cat /proc/uptime', safe=True)
        uptime_in_seconds = float(message.strip().split()[0])
        uptime = timedelta(seconds=uptime_in_seconds)
        self.logger.debug('uptime is [%s] on %s', uptime, self)
        return uptime

    def wait_for_reboot(self, reboot_time, retry_period=timedelta(seconds=1), timeout=timedelta(minutes=10)):
        def check_uptime():
            return self.uptime() < datetime.utcnow() - reboot_time

        wait_for(
            check_uptime,
            retry_period=retry_period,
            timeout=timeout,
            ignored_exceptions=[RemoteExecutionError, ValueError]
        )

    def reboot(self):
        self.logger.info('rebooting %s', self)
        reboot_time = datetime.utcnow()

        self.execute('nohup reboot &> /dev/null & exit', safe=False)
        if self.dry_run:
            # don't check, server has not been rebooted for real
            return
        self.wait_for_reboot(reboot_time)

    def execute(self, command, safe):
        self.logger.info('executing [%s] on %s', command, self)
        if self.dry_run and not safe:
            return
        worker = Transport.new(self.cumin_config, Target([self.fqdn]))
        if self.sudo:
            worker.commands = ['sudo ' + command]
        else:
            worker.commands = [command]
        worker.handler = 'sync'

        rc = worker.execute()

        message = None
        # we executed on a single node, there should be a single result
        for _, output in worker.get_results():
            message = output.message()
        self.logger.debug(message)
        return rc, message


class Icinga(Node):

    def __init__(self, fqdn, cumin_config, sudo, dry_run):
        super(Icinga, self).__init__(fqdn, cumin_config, dry_run, self, sudo)
        self.logger = logging.getLogger('estools.icinga')

    def downtime(self, node, duration, message):
        self.logger.info('scheduling downtime for %s', node)
        if self.dry_run:
            return

        rc, msg = self.execute('icinga-downtime -h {hostname} -d {duration} -r "{message}"'.format(
            hostname=node.hostname,
            duration=int(duration.total_seconds()),
            message=message
        ), safe=False)

        if rc != 0:
            raise RemoteExecutionError('Could not downtime %s: %s', node, msg)
