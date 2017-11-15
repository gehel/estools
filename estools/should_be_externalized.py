from cumin.transport import Transport
from datetime import timedelta, datetime

from cumin.transports import Target

from estools.utils import wait_for


class RemoteExecutionError(BaseException):
    pass


class Icinga(object):

    def __init__(self, fqdn, cumin_config, sudo):
        self.fqdn = fqdn
        self.cumin_config = cumin_config
        self.sudo = sudo

    def downtime(self, fqdn, duration, message):
        hostname = fqdn.split('.')[0]

        worker = Transport.new(self.cumin_config, Target([self.fqdn]))
        command = 'icinga-downtime -h {hostname} -d {duration} -r "{message}"'.format(
            hostname=hostname,
            duration=duration.total_seconds(),
            message=message
        )
        if self.sudo:
            worker.commands = ['sudo ' + command]
        else:
            worker.commands = [command]
        worker.handler = 'sync'

        rc = worker.execute()

        if rc != 0:
            raise RemoteExecutionError(rc)


class Node(object):

    def __init__(self, fqdn, cumin_config, dry_run, icinga, sudo):
        self.fqdn = fqdn
        self.cumin_config = cumin_config
        self.dry_run = dry_run
        self.icinga = icinga
        self.sudo = sudo

    def __repr__(self):
        return 'Node({node})'.format(node=self.fqdn)

    def run_puppet_agent(self):
        self.execute('run-puppet-agent', safe=False)

    def schedule_downtime(self, duration, message):
        self.icinga.downtime(self.fqdn, duration, message)

    def disable_puppet(self, message):
        self.execute('disable-puppet {message}'.format(message=message), safe=False)
        if self.is_puppet_enabled():
            raise RemoteExecutionError('Puppet still enabled.')

    def enable_puppet(self, message):
        self.execute('enable-puppet {message}'.format(message=message), safe=False)
        if not self.is_puppet_enabled():
            raise RemoteExecutionError('Puppet still disabled.')

    def is_puppet_enabled(self):
        try:
            self.execute('puppet-enabled', safe=True)
        except RemoteExecutionError as ree:
            if ree.args == 1:
                return False
            else:
                raise ree
        return True

    def depool(self):
        self.execute('depool', safe=False)

    def pool(self):
        self.execute('pool', safe=False)

    def stop_service(self, name):
        self.execute('service {name} stop'.format(name=name), safe=False)
        if self.is_service_running(name):
            raise RemoteExecutionError('Service {name} is still running.'.format(name=name))

    def start_service(self, name):
        self.execute('service {name} start'.format(name=name), safe=False)
        if not self.is_service_running(name):
            raise RemoteExecutionError('Service {name} is still stopped.'.format(name=name))

    def is_service_running(self, name):
        try:
            self.execute('service {name} status'.format(name=name), safe=False)
        except RemoteExecutionError as ree:
            if ree.args == 0:
                return True
            else:
                return False

    def upgrade_packages(self, packages):
        self.execute(
            'apt-get {options} install {packages}'.format(
                options='-o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold"',
                packages=' '.join(packages)
            ), safe=False)

    def uptime(self):
        uptime_in_seconds = float(self.execute('cat /proc/uptime', safe=True).strip().split()[0])
        return timedelta(seconds=uptime_in_seconds)

    def reboot(self):
        reboot_time = datetime.utcnow()

        def check_uptime():
            return self.uptime() < datetime.utcnow() - reboot_time

        self.execute('nohup reboot &> /dev/null & exit', safe=False)
        wait_for(
            check_uptime,
            retry_period=timedelta(seconds=1),
            timeout=timedelta(minutes=10),
            ignored_exceptions=[RemoteExecutionError]
        )

    def execute(self, command, safe):
        if self.dry_run and not safe:
            return
        worker = Transport.new(self.cumin_config, Target([self.fqdn]))
        if self.sudo:
            worker.commands = ['sudo ' + command]
        else:
            worker.commands = [command]
        worker.handler = 'sync'

        rc = worker.execute()
        if rc != 0:
            raise RemoteExecutionError(rc)

        message = None
        # we executed on a single node, there should be a single result
        for _, output in worker.get_results():
            message = output.message()
        return message
