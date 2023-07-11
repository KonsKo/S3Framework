"""Base client implementation."""
import constants
from helpers.cmd import ResultT, run_blocking
from helpers.exceptions import InvokingCommandError
from helpers.output_handler import OutputReaction


class BaseClient(object):
    """Base client class."""

    client_reaction: OutputReaction

    def __init__(self):
        if not isinstance(self.client_reaction, OutputReaction):
            raise AttributeError(
                'Attribute `client_reaction` must be derived from OutputReaction.',
            )

    def invoke_command(self, command: str) -> ResultT:
        """
        Invoke command.

        Args:
            command (str): command name

        Returns:
            (ResultT): result of command, error, process

        Raises:
            InvokingCommandError: if error of invoking of command
        """
        try:
            out, err, proc = run_blocking(command_args=command, lexical_analysis=True)

            self.client_reaction(
                msg='Out: {0}'.format(out),
                severity=constants.SEV_INFO,
            )

            return out, err, proc

        except Exception as exc_run:
            raise self.client_reaction(
                msg=[
                    'Failed to invoke command `{0}`'.format(
                        command,
                    ),
                    exc_run,
                ],
                severity=constants.SEV_ERROR,
                returned_exception=InvokingCommandError,
            )
