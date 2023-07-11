"""Module for client docker-compose."""
import json
import os
import time
from subprocess import Popen
from typing import Optional, Sequence, Union

from pydantic import BaseModel

import constants
from helpers import cmd, exceptions, output_handler

dc_reaction = output_handler.OutputReaction(
    prefix='DOCKER',
    module_name=__name__,
)


class RunningProcess(BaseModel):
    """
    Response structure of command `top`.

    https://docs.docker.com/engine/reference/commandline/compose_top/
    """

    uid: str
    pid: int
    ppid: int
    c: str
    stime: str
    tty: str
    time: str
    cmd: str


class DockerCompose(object):
    """
    Class to client: docker compose.

    Operations are based on compose file.
    """

    def __init__(self, compose_file: str):
        if os.path.isfile(compose_file):
            self.compose_file = compose_file
        else:
            raise TypeError('`compose_file` must be path to Docker Compose file.')

    def check_clean(self):
        # I was unable to find a good way to get a container name from port number.
        # Creation of pid file with predictable path could be an option.

        # Clean up possibly lingering containers
        self._invoke(command='down', options=['--timeout', '2'])
        # This code effectively means we can only run single instance of
        # the server for each docker-compose file.
        running_container = self.ps()
        if running_container:
            raise dc_reaction(
                msg=['Container is already running', running_container],
                severity=constants.SEV_WARNING,
                returned_exception=RuntimeError,
            )

    def up(self, detach: bool = True, options: str = ''):
        """
        Create and start containers.

        Args:
            detach (bool): run containers in the background
            options (str): command options
        """
        if detach:
            options = ['--detach', options]

        self._invoke(
            command='up',
            options=options,
        )

        self._wait_container_ready()

        dc_reaction(
            msg=['Container has been started', self.ps()],
            severity=constants.SEV_INFO,
        )

    def down(self):
        """Stop and remove containers, networks."""
        # docker-compose down sends SIGTERM, and after a timeout (10 sec) sends SIGKILL.
        # It might be not what we actually want;
        # https://docs.docker.com/compose/faq/#why-do-my-services-take-10-seconds-to-recreate-or-stop
        # The most correct way would be to use "docker-compose rm --froce here"
        # but for some reason it just doesn't work yet (self.ps() remains non-empty).
        self._invoke(command='down')

        if self.ps():
            raise dc_reaction(
                msg='Failed to stop container.',
                severity=constants.SEV_INFO,
                returned_exception=RuntimeError,
            )

        else:
            dc_reaction(
                msg='Stopped.',
                severity=constants.SEV_INFO,
            )

    def ps(self, options: str = '') -> dict:
        """
        List containers.

        Args:
            options (str): extra command options

        Returns:
            result (dict): containers
        """
        options = '--format json {0}'.format(options)

        out, proc = self._invoke(
            command='ps',
            options=options,
        )

        if out:

            try:
                return json.loads(out)

            except Exception as exc_loads:
                dc_reaction(
                    msg=['Failed to deserialize output', exc_loads],
                    severity=constants.SEV_ERROR,
                )

        return {}

    def restart(self, service: str):
        """
        Restart service containers.

        Args:
            service (str): service name to restart
        """
        dc_reaction(
            msg='Restarting service: {0}'.format(service),
            severity=constants.SEV_INFO,
        )

        out, proc = self._invoke(
            command='restart',
            options=service,
        )

        self._wait_container_ready()

        dc_reaction(
            msg='Restart result: {0}, {1}'.format(proc, out),
            severity=constants.SEV_INFO,
        )

    def top(self) -> list[RunningProcess]:
        """
        Display the running processes.

        Returns:
            response (list[ResponseTop]): result of command
        """
        out, proc = self._invoke(
            command='top',
        )

        return self._parse_response_of_top(out)

    def check_service_state(self, name: str) -> bool:
        """
        Check service (in project) state.

        Docker compose project may contain many services - check state of one.

        Args:
            name (str): service name

        Returns:
            State (bool): service state
        """
        for service in self.ps():
            if service.get('Service') == name and service.get('State') == 'running':

                dc_reaction(
                    msg=['Service state.', service],
                    severity=constants.SEV_INFO,
                )
                return True

        return False

    def send_signal(self, name, signal) -> bool:
        self._invoke(
            command='kill',
            options=['--signal', str(signal), name],
        )

    def _invoke(
        self, command: str, options: Union[str, Sequence, None] = None,
    ) -> tuple[str, str, Optional[Popen]]:
        """
        Invoke command.

        Args:
            command (str): command name
            options (Union[str, Sequence, None]): command options

        Returns:
            out, err, proc ([str, Optional[Popen]]): result of invoking and process

        Raises:
            exceptions.InvokingCommandError: if error of invoking of command
        """
        try:
            return cmd.run_blocking(
                command_args=self._make_command(
                    command=command,
                    command_attrs=options,
                ),
            )

        # docker considers warnings as errors.
        # i.e. it is not clear how to catch warnings (not all of them contain word `Warning`)
        except exceptions.InvokingCommandError as err_invoke:
            dc_reaction(
                msg=[
                    'Caught error from invoke command `{0}` with options `{1}`'.format(
                        command,
                        str(options) if options else '',
                    ),
                    err_invoke,
                ],
                severity=constants.SEV_WARNING,
            )

        except Exception as exc_run:
            raise dc_reaction(
                msg=[
                    'Failed to invoke command `{0}` with options `{1}`'.format(
                        command,
                        str(options) if options else '',
                    ),
                    exc_run,
                ],
                severity=constants.SEV_ERROR,
                returned_exception=RuntimeError,
            )

    def _make_command(
        self,
        command: str,
        command_attrs: Union[str, Sequence, None] = None,
    ) -> str:
        """
        Make command line.

        Args:
            command (str): command name
            command_attrs (Optional[list]): command parameters

        Returns:
            command_line (str): command line
        """
        if command_attrs and not isinstance(command_attrs, str):
            command_attrs = ' '.join(command_attrs)

        base_command = 'docker-compose -f {0}'.format(self.compose_file)

        command = '{base} {command} {attrs}'.format(
            base=base_command,
            command=command,
            attrs=command_attrs if command_attrs else '',
        )

        dc_reaction(
            msg='Command to invoke: {0}'.format(command),
            severity=constants.SEV_INFO,
        )

        return command.strip()

    def _wait_container_ready(self):
        """Wait container is ready."""
        attempt = 0

        while attempt < 5:

            if self.ps():
                break

            time.sleep(1)
            attempt += 1

            dc_reaction(
                msg='Waiting container: attempt {0}'.format(attempt),
                severity=constants.SEV_WARNING,
            )

        else:
            dc_reaction(
                msg='Container is not ready',
                severity=constants.SEV_WARNING,
            )

    @staticmethod
    def _parse_response_of_top(response: str) -> list[RunningProcess]:
        """
        Parse response of command `top`.

        Args:
            response (str): response to parse

        Returns:
            response (list[ResponseTop]): parsed response
        """
        response = response.split('\n')

        parsed_response = []

        for row in response:
            if not row:  # empty
                continue

            row = row.split()

            if len(row) == 1:  # service name
                continue

            if row[0].strip() == 'UID':  # headers
                continue

            try:
                # docker compose running process structure, by the docs
                parsed_response.append(
                    RunningProcess(
                        uid=row[0].strip(),
                        pid=int(row[1].strip()),
                        ppid=int(row[2].strip()),
                        c=row[3].strip(),
                        stime=row[4].strip(),
                        tty=row[5].strip(),
                        time=row[6].strip(),
                        cmd=row[7].strip(),
                    ),
                )
            except Exception as exc:
                raise dc_reaction(
                    msg=['Failed to parse response of command `top`', exc],
                    severity=constants.SEV_WARNING,
                    returned_exception=RuntimeError,
                )

        return parsed_response
