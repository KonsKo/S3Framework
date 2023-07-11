"""Server S3 module."""
import fcntl
import os
import signal
import socket
import struct
import time
from abc import ABC, abstractmethod
from subprocess import PIPE
from typing import TYPE_CHECKING, Optional, Union

import psutil
from pydantic import BaseModel, root_validator, validator

import constants
from clients.docker_compose import DockerCompose, RunningProcess
from clients.request import make_request
from helpers import cmd, exceptions, framework, output_handler, utils

# import for type hinting only by https://peps.python.org/pep-0484/
if TYPE_CHECKING:
    from clients.awsboto import Boto

s3_reaction = output_handler.OutputReaction(
    module_name=__name__,
    prefix=constants.S3SERVER_PROC_NAME,
)

MAX_PORT = 65535
MIN_PORT = 1024
truncated_logs = {}

OUT_SERVER_STARTED_S = 'Server has been started successfully'


class ServerS3Config(BaseModel):
    """<> S3 config class."""

    src: str = constants.S3SERVER_DEFAULT_SRC
    listen_address: str = constants.S3SERVER_DEFAULT_IP
    listen_port: str = constants.S3SERVER_DEFAULT_PORT
    tls_cert: Optional[str] = None
    tls_key: Optional[str] = None
    no_tls: bool = False
    no_keepalive: bool = False
    background: bool = False
    log: Optional[str] = None
    cpuset: Optional[str] = None
    s3root: str = '/s3'
    verbose: bool = False
    hosts: Optional[list] = None
    server_header: Optional[str] = None
    monitoring_port: Optional[str] = None
    extra_parameter: Optional[str] = None
    seclib: Optional[str] = None
    vfs: bool = True
    override_parameter: Optional[str] = None
    tcp_nodelay: Optional[str] = None
    req_limit: Optional[str] = None

    compose_file: Optional[str] = None
    compose_service: Optional[str] = None

    class Config:
        validate_assignment = True

    @root_validator
    def _check_tls_data(cls, conf_values):
        # if flag `no_tls` was provided, we do not need certs and keys
        if conf_values.get('no_tls'):
            conf_values['tls_cert'] = None
            conf_values['tls_key'] = None

        # if flag `no_tls` was NOT provided, we need both: cert and key
        else:
            if conf_values.get('tls_cert') and not conf_values.get('tls_key'):
                raise ValueError('No TLS key was provided.')
            elif conf_values.get('tls_key') and not conf_values.get('tls_cert'):
                raise ValueError('No TLS certificate was provided.')

        return conf_values

    @validator('listen_address', pre=True, always=True)
    def _validate_address(cls, field_value: str):
        return field_value or constants.S3SERVER_DEFAULT_IP

    @validator('listen_port', pre=True, always=True)
    def _validate_port(cls, field_value: str):
        # port number validation
        if int(field_value) < MIN_PORT or int(field_value) > MAX_PORT:
            raise ValueError(
                'Port must be in range from {0} to {1}, got {2}'.format(
                    MIN_PORT,
                    MAX_PORT,
                    field_value,
                ),
            )
        return field_value or constants.S3SERVER_DEFAULT_PORT

    @validator('src', pre=True, always=True)
    def _validate_src(cls, field_value: str):
        if not field_value:
            return constants.S3SERVER_DEFAULT_SRC
        return field_value


class BaseServerS3(ABC):
    """ABC class for s3 server."""

    @abstractmethod
    def __init__(self, config: Union[ServerS3Config, dict]):
        """
        Init class instance.

        Args:
            config (Union[<>S3Config): <> s3 config
        """

    @abstractmethod
    def start(self, fast_start: bool = False) -> None:
        """
        Start process `<>s3` from command line.

        Args:
            fast_start (bool): flag to ignore server readiness
        """

    @abstractmethod
    def stop(self) -> None:
        """Stop `<>s3` process."""

    @abstractmethod
    def setpriority(self, value: int) -> None:
        """
        Set server process scheduling priority.

        Args:
            value (int): value for priority
        """

    @abstractmethod
    def is_running(self) -> bool:
        """
        Check server is running.

        Returns:
            state (bool): state of server process.
        """

    @abstractmethod
    def send_health(self) -> int:
        """
        Send health request to server.

        Returns:
            status (int): http status of response
        """

    @abstractmethod
    def restart(self) -> None:
        """Restart server."""

    @abstractmethod
    def read_log(self, tail: Optional[int] = None) -> list[str]:
        """
        Read log file and return log data.

        Args:
            tail (Optional[int]): last `tail` rows in log file

        Returns:
            log_data (list[str]): log file data
        """

    @abstractmethod
    def remove_log(self) -> None:
        """Remove log file."""

    @abstractmethod
    def get_effective_root(self) -> str:
        """
        Return path to real s3 root directory.

        Returns:
            s3root (str): real s3 root directory path
        """

    @abstractmethod
    def create_endpoint_url(self, with_port: bool = True) -> str:
        """
        Create endpoint url is based on current server specifications.

        Args:
            with_port (bool): include port to url

        Returns:
            url (str): endpoint url for current specifications
        """


ERROR_MSG_NOT_IMPLEMENTED = 'Class does not support such method: server is considered as external.'


class ServerS3(BaseServerS3):
    """
    Default Server S3 class implementation.

    Default class has no implemented methods related to start-stop-processing of server.

    Best use-case: external server such real AWS.
    """

    def __init__(self, config: Union[ServerS3Config, dict]):
        """
        Init class instance.

        Args:
            config (Union[<>S3Config): <> s3 config
        """
        self.config = ServerS3Config(**config) if isinstance(config, dict) else config
        self.process: Optional[psutil.Popen] = None
        # Optional Boto instances to close on stop().
        self.linked_boto: list['Boto'] = []

        s3_reaction(
            msg='Server was initialised as class `{0}`'.format(
                type(self).__name__,
            ),
            severity=constants.SEV_INFO,
        )

    def start(self, fast_start: bool = False) -> None:
        """
        Start server instance.

        Method is not implemented by default, because we can not start aws server,
        end current work with <>s3 <>fs does not imply such action too.

        Args:
            fast_start (bool): flag to ignore server readiness

        Raises:
            NotImplementedError: always
        """
        raise s3_reaction(
            msg=ERROR_MSG_NOT_IMPLEMENTED,
            severity=constants.SEV_EXCEPTION,
            returned_exception=NotImplementedError,
        )

    def setpriority(self, value: int) -> None:
        """
        Set server process scheduling priority.

        Method is not implemented by default, reason is same as for `start`.

        Args:
            value (int): value for priority

        Raises:
            NotImplementedError: always
        """
        raise s3_reaction(
            msg=ERROR_MSG_NOT_IMPLEMENTED,
            severity=constants.SEV_EXCEPTION,
            returned_exception=NotImplementedError,
        )

    def stop(self) -> None:
        """
        Stop server instance.

        Method is not implemented by default, reason is same as for `start`.

        Raises:
            NotImplementedError: always
        """
        raise s3_reaction(
            msg=ERROR_MSG_NOT_IMPLEMENTED,
            severity=constants.SEV_EXCEPTION,
            returned_exception=NotImplementedError,
        )

    def restart(self) -> None:
        """
        Restart server.

        Method is not implemented by default, restarting process is different.

        Raises:
            NotImplementedError: always
        """
        raise s3_reaction(
            msg=ERROR_MSG_NOT_IMPLEMENTED,
            severity=constants.SEV_EXCEPTION,
            returned_exception=NotImplementedError,
        )

    def is_running(self) -> bool:
        """
        Check server is running.

        Returns:
            state (bool): state of server process.
        """
        if self.process:

            # psutil is actually unable to detect a zombie
            return self.process.is_running() and self.process.status() != psutil.STATUS_ZOMBIE

        return False

    def send_health(self) -> int:
        """
        Send health request to server.

        Returns:
            status (int): http status of response
        """
        health_url = '{endpoint}/{health}'.format(
            endpoint=self.create_endpoint_url(),
            health=constants.S3SERVER_HEALTH_URL,
        )
        try:
            response = make_request(
                url=health_url,
                ca_cert=self.config.tls_cert,
                mask_connection_reset=False,
            )
        # it is sign to something happened with connection
        except ConnectionError as conn_err:
            raise s3_reaction(
                msg=['There is problem with connection.', conn_err],
                severity=constants.SEV_EXCEPTION,
                returned_exception=exceptions.LostConnectionError,
            )

        return response.status

    def read_log(self, tail: Optional[int] = None) -> list[str]:
        """
        Read log file and return log data.

        Method is not implemented by default, reason is same as for `start`.

        Args:
            tail (Optional[int]): last `tail` rows in log file

        Raises:
            NotImplementedError: always
        """
        raise s3_reaction(
            msg=ERROR_MSG_NOT_IMPLEMENTED,
            severity=constants.SEV_EXCEPTION,
            returned_exception=NotImplementedError,
        )

    def remove_log(self) -> None:
        """
        Remove log file.

        Method is not implemented by default, reason is same as for `start`.

        Raises:
            NotImplementedError: always
        """
        raise s3_reaction(
            msg=ERROR_MSG_NOT_IMPLEMENTED,
            severity=constants.SEV_EXCEPTION,
            returned_exception=NotImplementedError,
        )

    def get_effective_root(self) -> str:
        """
        Return path to real s3 root directory.

        Method is not implemented by default, reason is same as for `start`.

        Raises:
            NotImplementedError: always
        """
        raise s3_reaction(
            msg=ERROR_MSG_NOT_IMPLEMENTED,
            severity=constants.SEV_EXCEPTION,
            returned_exception=NotImplementedError,
        )

    def create_endpoint_url(self, with_port: bool = True) -> str:
        """
        Create endpoint url is based on current server specifications.

        Args:
            with_port (bool): include port to url

        Returns:
            url (str): endpoint url for current specifications
        """
        if self.config.hosts:
            host_name = self.config.hosts[0]  # take first host from hosts
        else:
            host_name = self.config.listen_address

        return 'http{ssl}://{host}{colon}{port}'.format(
            ssl='s' if self.config.tls_cert else '',
            host=host_name,
            colon=':' if with_port else '',
            port=self.config.listen_port if with_port else '',
        )

    def clean_linked_boto(self):
        """
        Clean all linked boto instances.

        Boto uses persistent connections, which block <>s3 shutdown
        """
        old_boto_list = self.linked_boto
        self.linked_boto = []
        if old_boto_list:
            for inst in old_boto_list:
                inst.close()

    def _wait_server_ready(self):
        """
        Wait success response from server on health command.

        Raises:
            InvokingCommandError: if error happened and <>s3 has been stopped
        """
        status = -1
        step = 0.5
        t_time = 0
        time.sleep(0.1)
        t_time += 0.1
        while t_time < constants.S3SERVER_TIMEOUT:  # noqa:E501

            try:
                status = self.send_health()
                break  # success

            except socket.timeout:
                s3_reaction(
                    msg='Timeout: server is NOT ready.',
                    severity=constants.SEV_WARNING,
                )
                t_time += (step + constants.HTTP_TIMEOUT)
                time.sleep(step)

            except (ConnectionRefusedError, exceptions.LostConnectionError):
                # instant error
                t_time += step
                time.sleep(step)

            if not self.is_running():
                break  # <>s3 is dead, do not try again

        if not self.is_running():
            raise s3_reaction(
                msg='Server terminated unexpectedly.',
                severity=constants.SEV_CRITICAL,
                returned_exception=exceptions.ServerIsNotStarted,
            )

        if status != constants.HTTP_STAT.OK:
            self.stop()
            raise s3_reaction(
                msg='Server has been stopped due timeout: not ready (status = {}).'.format(status),
                severity=constants.SEV_CRITICAL,
                returned_exception=exceptions.InvokingCommandError,
            )

        s3_reaction(
            msg='Waiting the server: server is ready.',
            severity=constants.SEV_INFO,
        )

    def server_log_path(self) -> str:
        return self.config.log

    def _make_command_line(self) -> str:
        """
        Create command line to start `<>s3`.

        Returns:
            cmd(str): created command line
        """
        command_line = self.config.src

        if self.config.seclib:
            command_line += ' --seclib {0}'.format(
                self.config.seclib,
            )
        if self.config.listen_address:
            command_line += ' --listen-address {0}'.format(
                self.config.listen_address,
            )
        if self.config.listen_port:
            command_line += ' --listen-port {0}'.format(
                self.config.listen_port,
            )
        if self.config.tls_cert:
            command_line += ' --tls-cert {0}'.format(
                self.config.tls_cert,
            )
        if self.config.tls_key:
            command_line += ' --tls-key {0}'.format(
                self.config.tls_key,
            )
        if self.config.tcp_nodelay:
            command_line += ' --tcp-nodelay {0}'.format(
                self.config.tcp_nodelay,
            )
        if self.config.req_limit:
            command_line += ' --req-limit {0}'.format(
                self.config.req_limit,
            )
        if self.config.no_tls:
            command_line = '{0} --no-tls'.format(command_line)
        if self.config.no_keepalive:
            command_line = '{0} --no-keepalive'.format(command_line)
        if self.config.background:
            command_line = '{0} --background'.format(command_line)
        if self.server_log_path():
            command_line += ' --log {0}'.format(
                self.server_log_path(),
            )
        if self.config.cpuset:
            command_line += ' --cpuset {0}'.format(
                self.config.cpuset,
            )
        if self.config.hosts:
            for hostname in self.config.hosts:
                command_line += ' --host {0}'.format(
                    hostname,
                )
        if self.config.verbose:
            command_line = '{0} --verbose'.format(command_line)

        if not framework.structure.is_external_server:
            command_line += ' --s3root {0}'.format(
                self.get_effective_root(),
            )

        if self.config.server_header:
            command_line += ' --server-header {0}'.format(self.config.server_header)

        if self.config.monitoring_port:
            command_line += ' --monitoring-port {0}'.format(self.config.monitoring_port)

        if not framework.structure.is_external_server:
            command_line = '{0} --vfs'.format(command_line)

        if self.config.extra_parameter:
            command_line += ' {0}'.format(self.config.extra_parameter)

        if self.config.override_parameter:
            if self.config.override_parameter == ' ':
                command_line = self.config.src
            else:
                if framework.structure.is_external_server:
                    command_line = '{0} {1}'.format(
                        self.config.src,
                        self.config.override_parameter,
                    )
                else:
                    command_line = '{0} --vfs {1}'.format(
                        self.config.src,
                        self.config.override_parameter,
                    )

        s3_reaction(
            msg='Cmd: {0}.'.format(command_line),
            severity=constants.SEV_INFO,
        )

        return command_line


class S3(ServerS3):
    """
    <> S3 class implementation.

    Class inherited from base one with implemented start-stop-processing methods.
    """

    def start(self, fast_start: bool = False) -> None:
        """
        Start process `<>s3` from command line.

        Find process by pid and represent it as `psutil.Process`.

        Args:
            fast_start (bool): flag to ignore server readiness

        Raises:
            boto_reaction: if error happened
        """
        # <>s3 always appends to the log, so we should truncate manually
        if self.config.log not in truncated_logs and os.access(self.config.log, os.W_OK):
            truncated_logs[self.config.log] = True
            try:
                open(self.config.log, 'w').close()
            except Exception as exc:
                s3_reaction(
                    msg='Failed to truncate <>s3 log "{}": {}'.format(
                        self.config.log,
                        exc,
                    ),
                    severity=constants.SEV_WARNING,
                )

        utils.check_port(
            port=self.config.listen_port,
            force_stop=True,
        )

        cmd_server = self._make_command_line()

        # bypass permission checks (for root) to allow `chmod` has effect for root user
        if framework.structure.run_as_root_user and framework.structure.drop_cap:

            cmd_server = 'capsh --drop=cap_dac_override,cap_dac_read_search -- -c "{0}"'.format(
                cmd_server,
            )
            s3_reaction(
                msg=[
                    'CMD to start server has been modified:',
                    cmd_server,
                ],
                severity=constants.SEV_SYSTEM,
            )

        if self.config.background:
            self.process = cmd.run_nonblocking(
                command_args=cmd_server,
                lexical_analysis=True,
                stdout=PIPE,
            )
            self._modify_to_background()
        else:
            self.process = cmd.run_nonblocking(
                command_args=cmd_server,
                lexical_analysis=True,
            )

        # add server to storage right after creation to clean it in any case
        framework.structure.add_s3(self)

        if not fast_start:
            self._wait_server_ready()

        s3_reaction(
            msg='{0}{1}. Process: {2}.'.format(
                OUT_SERVER_STARTED_S,
                ' as daemon' if self.config.background else '',
                self.process,
            ),
            severity=constants.SEV_SYSTEM,
        )

    def setpriority(self, value: int) -> None:
        """
        Set server process scheduling priority.

        Args:
            value (int): value for priority
        """
        # Doable for containers via "sudo nice ..."
        if self.process:
            for thread in self.process.threads():
                os.setpriority(os.PRIO_PROCESS, thread.id, value)

    def stop(self) -> None:
        """Stop `<>s3` process."""
        self.clean_linked_boto()

        # Do not try to kill/wait the process if it failed once
        framework.structure.remove_s3(self)

        if self.process:
            if self.process.is_running():
                shutdown_s3_process(self.process, self.config.listen_port)
            else:
                s3_reaction(
                    msg='Cannot stop, server is NOT running: {0}'.format(
                        self.process,
                    ),
                    severity=constants.SEV_WARNING,
                )

        else:
            s3_reaction(
                msg='Stopping: `process` does not exist. Nothing to stop.',
                severity=constants.SEV_INFO,
            )

    def restart(self) -> None:
        """
        Restart server.

        Raises:
            NotImplementedError: always
        """
        raise s3_reaction(
            msg='<> S3 has no restart. In progress.',
            severity=constants.SEV_EXCEPTION,
            returned_exception=NotImplementedError,
        )

    def read_log(self, tail: Optional[int] = None) -> list[str]:
        """
        Read log file and return log data.

        Args:
            tail (Optional[int]): last `tail` rows in log file

        Returns:
            log_data (list[str]): log file data
        """
        log_data = []

        if self.config.log:
            with open(self.config.log) as log_file:
                log_data = log_file.readlines()
            if log_data and tail:
                tail = tail if tail <= len(log_data) else len(log_data)
                return log_data[-1:-int(tail):-1]

        return log_data

    def remove_log(self):
        """Remove log file."""
        try:
            os.remove(
                self.config.log,
            )
        except Exception:
            s3_reaction(
                msg='Failed to remove old log file.',
                severity=constants.SEV_WARNING,
            )

    def get_effective_root(self) -> str:
        """
        Return path to real s3 root directory.

        Real directory placed inside temporary directory,

        Returns:
            s3root (str): real s3 root directory path
        """
        return os.path.join(
            constants.TMP_DIR,
            self.config.s3root.strip('/'),
        )

    def _modify_to_background(self):
        """
        Modify instance of <>s3 if background option was provided.

        Find background process, wait foreground process, update self.process instance.
        """
        s3_reaction(
            msg='Foreground process: {0}.'.format(self.process),
            severity=constants.SEV_SYSTEM,
        )
        try:
            out, err = self.process.communicate(timeout=constants.S3SERVER_TIMEOUT)
        except psutil.TimeoutExpired:
            self.process.kill()
            self.process.communicate()
            raise s3_reaction(
                msg='Server failed to finish and was killed. PID {0}.'.format(
                    self.process.pid,
                ),
                severity=constants.SEV_WARNING,
                returned_exception=exceptions.FrameworkRunTimeError,
            )

        if self.process.returncode == 0:
            # <>s3 foreground process prints background process pid to stdout
            bg_pid = int(out)
            # get background process handle
            bg_proc = utils.find_process_by_pid(bg_pid)
            # replace foreground process handle with background process handle
            self.process = bg_proc
        else:
            raise s3_reaction(
                msg='Foreground process with PID {0} exited with {1}.'.format(
                    self.process.pid,
                    self.process.returncode,
                ),
                severity=constants.SEV_CRITICAL,
                returned_exception=exceptions.InvokingCommandError,
            )


class S3FS(ServerS3):
    """
    Class for <> S3 server running with <> FS.

    All processes are based on `docker-compose` commands, because
    we know that such server instance run inside a container.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.config.compose_file:
            raise s3_reaction(
                msg='Config field `compose_file` is required to run server with <> FS mode',
                severity=constants.SEV_INFO,
                returned_exception=RuntimeError,
            )
        self._dc = DockerCompose(
            compose_file=self.config.compose_file,
        )

    def start(self, fast_start: bool = False) -> None:
        """
        Start container with `<>s3`.

        Args:
            fast_start (bool): flag to ignore server readiness
        """
        os.environ['<>S3_CMD'] = self._make_command_line()
        if 'WORKSPACE' not in os.environ:
            os.environ['WORKSPACE'] = os.path.dirname(os.path.dirname(os.path.abspath(constants.WORK_DIR)))

        self._dc.check_clean()
        self._dc.up()

        assert self._dc.check_service_state(name=self.config.compose_service)

        self.process = self._find_process_by_cmd(source=self._dc.top())

        framework.structure.add_s3(self)

        if not fast_start:
            self._wait_server_ready()

        s3_reaction(
            msg='{0}. Process: {1}'.format(
                OUT_SERVER_STARTED_S,
                self.process,
            ),
            severity=constants.SEV_INFO,
        )

    def stop(self) -> None:
        """Stop container with server."""
        # super().stop()
        # TODO server is running under docker,
        # TODO if we try to send signal to process i.e. self.process.send_signal(signal.SIGINT)
        # TODO we will get AccessDenied

        # - We can try to stop server process with `docker-compose exec`
        # - We can try to run docker with `Rootless mode`

        # TODO DO we need stop server process before container stopping?
        old_boto_list = self.linked_boto
        self.linked_boto = []
        if old_boto_list:
            for inst in old_boto_list:
                inst.close()

        framework.structure.remove_s3(self)

        if self.process:
            s3_reaction(
                msg='Server state before `docker compose down` is {0}running. Process {1}.'.format(
                    '' if self.process.is_running() else 'NOT ',
                    self.process,
                ),
                severity=constants.SEV_INFO,
            )

        shutdown_s3_container(self.process, self._dc, self.config.compose_service)
        self._dc.down()

        s3_reaction(
            msg='Server state after `docker compose down` is {0}running. Process {1}.'.format(
                '' if self.process.is_running() else 'NOT ',
                self.process,
            ),
            severity=constants.SEV_INFO,
        )

    def restart(self) -> None:
        """
        Restart server.

        Actually, action restarts container in which server lives.
        """
        self._dc.restart(service=self.config.compose_service)

    @staticmethod
    def _find_process_by_cmd(source: list[RunningProcess]) -> psutil.Process:
        """
        Find process by CMD.

        Args:
            source (list[RunningProcess]): parsed result of `docker compose top`

        Returns:
            process (psutil.Process): Process instance
        """
        for row in source:

            # `cmd` - abs path to executable
            if row.cmd.endswith(constants.S3SERVER_PROC_NAME):

                return utils.find_process_by_pid(pid=row.pid)

    def server_log_path(self) -> str:
        # Replace host path with container path.
        # Container path is hardcoded via docker-compose.yml
        return self.config.log.replace(constants.WORK_DIR, '/<>/logs', 1)


def shutdown_s3_process(
    process: Union[psutil.Process, psutil.Popen],
    port: Union[int, str],
):
    process.send_signal(signal.SIGINT)

    to_kill = False
    try:
        process.wait(timeout=constants.S3SERVER_KILL_TIMEOUT)
    except psutil.TimeoutExpired:
        if process.is_running():  # Otherwise already finished
            to_kill = True

    if to_kill:
        signal_failed = False
        process.send_signal(signal.SIGILL)
        try:
            process.wait(timeout=1)
        except psutil.TimeoutExpired:
            if process.is_running():  # Otherwise already finished
                signal_failed = True

        if signal_failed:
            s3_reaction(
                msg='Failed to terminate the server with SIGILL. PID {0}.'.format(
                    process.pid,
                ),
                severity=constants.SEV_ERROR,
            )
            process.kill()
            process.wait(timeout=1)

        time.sleep(0.1)  # delaying to update data

    # Don't try to kill it second time
    utils.check_port(
        port=int(port),
        force_stop=False,
    )

    if to_kill:
        raise s3_reaction(
            msg='Server failed to finish and was killed. PID {0}.'.format(
                process.pid,
            ),
            severity=constants.SEV_WARNING,
            returned_exception=exceptions.FrameworkRunTimeError,
        )
    else:
        s3_reaction(
            msg='Server has been stopped. PID {0}.'.format(
                process.pid,
            ),
            severity=constants.SEV_SYSTEM,
        )


# We cannot directly send signals to containers that run as root
def shutdown_s3_container(
    process: Union[psutil.Process, psutil.Popen],
    containers: DockerCompose,
    service_name: str,
):
    containers.send_signal(service_name, int(signal.SIGINT))

    to_kill = False
    try:
        process.wait(timeout=constants.S3SERVER_KILL_TIMEOUT)
    except psutil.TimeoutExpired:
        if process.is_running():  # Otherwise already finished
            to_kill = True

    if to_kill:
        signal_failed = False
        containers.send_signal(service_name, int(signal.SIGILL))
        try:
            process.wait(timeout=1)
        except psutil.TimeoutExpired:
            if process.is_running():  # Otherwise already finished
                signal_failed = True

        if signal_failed:
            s3_reaction(
                msg='Failed to terminate the server with SIGILL. PID {0}.'.format(
                    process.pid,
                ),
                severity=constants.SEV_ERROR,
            )
            containers.send_signal(service_name, int(signal.SIGKILL))
            process.wait(timeout=1)

        time.sleep(0.1)  # delaying to update data

    if to_kill:
        raise s3_reaction(
            msg='Server\'s container failed to finish and was killed. PID {0}.'.format(
                process.pid,
            ),
            severity=constants.SEV_WARNING,
            returned_exception=exceptions.FrameworkRunTimeError,
        )
    else:
        s3_reaction(
            msg='Server\'s container has been stopped. PID {0}.'.format(
                process.pid,
            ),
            severity=constants.SEV_SYSTEM,
        )


def find_s3_by_logfile(filename: str):
    tmpdir = os.environ.get('TMPDIR', '/tmp')
    # We are sure the file exists, so we don't catch any errors here
    with open(filename, 'r') as pidfile:
        # type, whence, start, len, pid
        lockdata = struct.pack('hhnni', fcntl.F_WRLCK, 0, 0, 1, 0)
        res = fcntl.fcntl(pidfile, fcntl.F_GETLK, lockdata)
        _, _, _, _, pid = struct.unpack('hhnni', res)
        return pid


def find_s3_by_address(listen_ip: str, listen_port: int):
    tmpdir = os.environ.get('TMPDIR', '/tmp')
    # We are sure the file exists, so we don't catch any errors here
    return find_s3_by_logfile('{}/s3-{}-{}'.format(tmpdir, listen_ip, listen_port))
