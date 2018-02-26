import logging

from cumin.transport import Transport
from datetime import timedelta, datetime

from cumin.transports import Target

from estools.utils import wait_for


class RemoteExecutionError(Exception):
    pass


class Nodes(object):

    def __init__(self, fqdns, cumin_config, dry_run, icinga, sudo):
        assert isinstance(fqdns, list)
        self.fqdns = fqdns
        self._target = Target(fqdns)
        self.cumin_config = cumin_config
        self.dry_run = dry_run
        self.icinga = icinga
        self.sudo = sudo
        self.logger = logging.getLogger('estools.node')

    def __repr__(self):
        """
        >>> Nodes(['myhost.example.net', 'myotherhost.example.net'], cumin_config={}, dry_run=True, icinga=None, sudo=False)
        Node(myhost.example.net,myotherhost.example.net)
        >>> Nodes(['host1.example.net', 'host2.example.net', 'host3.example.net'], cumin_config={}, dry_run=True, icinga=None, sudo=False)
        Node(host[1-3].example.net)
        """
        return 'Node({nodes})'.format(nodes=self._target.hosts)

    def hostnames(self):
        """
        >>> nodes = Nodes(['myhost.example.net', 'myotherhost.example.net'], cumin_config={}, dry_run=True, icinga=None, sudo=False)
        >>> nodes.hostnames()
        ['myhost', 'myotherhost']
        """
        return [f.split('.')[0] for f in self.fqdns]

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
        rc, _ = self.execute('service {name} status'.format(name=name), safe=True)
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
            ),
            safe=False)

    def max_uptime(self):
        def parse_uptime(message):
            uptime_in_seconds = float(message.strip().split()[0])
            return timedelta(seconds=uptime_in_seconds)

        _, results = self.execute('cat /proc/uptime', safe=True, transform=parse_uptime)

        for hosts, uptime in results:
            self.logger.debug('uptime is [%s] on %s', uptime, hosts)
        return max([uptime for _, uptime in results])

    def wait_for_reboot(self, reboot_time, retry_period=timedelta(seconds=1), timeout=timedelta(minutes=10)):
        def check_uptime():
            return self.max_uptime() < datetime.utcnow() - reboot_time

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

    def execute_single(self, command, safe):
        rc, results = self.execute(command, safe)
        # we executed on a single node, there should be a single result
        for _, message in results:
            return rc, message

    def execute(self, command, safe, transform=lambda x: x):
        self.logger.info('executing [%s] on %s', command, self)
        if self.dry_run and not safe:
            return 0, [(None, '')]
        worker = Transport.new(self.cumin_config, self._target)
        if self.sudo:
            worker.commands = ['sudo ' + command]
        else:
            worker.commands = [command]
        worker.handler = 'sync'

        rc = worker.execute()

        return rc, [(host, transform(result.message().decode('utf-8'))) for host, result in worker.get_results()]


class Icinga(Nodes):

    def __init__(self, fqdn, cumin_config, sudo, dry_run):
        super(Icinga, self).__init__([fqdn], cumin_config, dry_run, self, sudo)
        self.logger = logging.getLogger('estools.icinga')

    def downtime(self, nodes, duration, message):
        self.logger.info('scheduling downtime for %s', nodes)

        for hostname in nodes.hostnames():
            rc, _ = self.execute('icinga-downtime -h {hostname} -d {duration} -r "{message}"'.format(
                hostname=hostname,
                duration=int(duration.total_seconds()),
                message=message
            ), safe=False)

            if rc != 0:
                raise RemoteExecutionError('Could not downtime %s', nodes)
