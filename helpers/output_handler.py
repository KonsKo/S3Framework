"""Display module."""
import datetime
import logging
import sys
from typing import Optional, Type, Union

import constants


class OutputReaction(object):
    """Class for process info while testing."""

    severity_to_colors = {
        'error': '\033[33m',  # yellow
        'info': '\033[0;32m',  # green
        'exception': '\033[1;34m',  # blue
        'warning': '\033[1;36m',  # cyan
        'critical': '\033[1;31m',  # red
        constants.SEV_EXTRA: '\033[35m',  # purple
        constants.SEV_SYSTEM: '\033[35m',  # magenta
    }
    reset = '\033[0;0m'
    custom_severities = {constants.SEV_EXTRA, constants.SEV_SYSTEM}

    def __init__(
        self,
        module_name: str = __name__,
        prefix: str = 'output',
        display_info: Optional[bool] = True,
    ):
        """
        Init class instance.

        Args:
            module_name (str): name of module
            prefix (str): prefix for console message
            display_info (Optional[bool]): display info to console

        """
        self.child_logger = None
        self.create_child_logger(module_name=module_name)
        self.prefix = prefix
        self.display_info = display_info

    def __call__(
        self,
        msg: Union[str, list],
        severity: str,
        returned_exception: Optional[Type[Exception]] = None,
        prefix: Optional[str] = None,
        no_log: Optional[bool] = False,
        no_screen: Optional[bool] = False,
    ) -> Exception:
        """
        Call method.

        Args:
            msg (Union[str, list]): message
            severity (str): severity
            returned_exception (Optional[Type[Exception]]): exception to return
            prefix (Optional[str]): if needed change prefix
            no_log (Optional[bool]): if do not need to log
            no_screen (Optional[bool]): if do not need to display

        Returns:
            return_exception (Exception):
                exception to raise with predefined msg
        """
        if self.display_info and not no_screen:
            self.display_on_screen(severity=severity, msg=msg)

        if isinstance(msg, list):
            msg = '\n'.join(
                [str(element) for element in msg],
            )

        if not no_log:
            self.log(severity=severity, msg=msg)

        if returned_exception:
            if issubclass(returned_exception, Exception):
                return returned_exception(msg)

            self.display_on_screen(
                msg='Failed to process exception: {0}. Wrong type.'.format(
                    returned_exception,
                ),
                severity='warning',
            )
            # if returned_exception is not of Exception type
            return Exception(msg)

        # if returned_exception is None
        return Exception(msg)

    def log(self, severity: str, msg: str):
        """
        Log msg.

        Args:
            msg (str): message
            severity (str): severity
        """
        if self.child_logger:

            if severity in self.custom_severities:
                return

            elif hasattr(self.child_logger, severity):  # noqa:WPS421
                to_call = getattr(self.child_logger, severity)
                to_call(msg)

            else:
                self.display_on_screen(
                    msg='Wrong severity call.',
                    severity=constants.SEV_ERROR,
                )
        else:
            self.display_on_screen(
                msg='Logger does not exist.',
                severity=constants.SEV_ERROR,
            )

    def display_on_screen(self, msg: Union[str, list[str]], severity: str):
        """
        Display message (before first new line) to console.

        Args:
            msg (str): message
            severity (str): severity

        """
        if isinstance(msg, str):
            msg = [msg]
        for message in msg:
            sys.stdout.write(
                self.create_screen_msg(
                    msg=message,
                    severity=severity,
                ),
            )

    def create_screen_msg(self, msg: str, severity: str) -> str:
        """
        Prepare message to console output.

        Example: [REDS3     ][ERROR     ]<msg>

        Args:
            msg (str): message
            severity (str): severity

        Returns:
            msg (str): prepared message
        """
        return '{color}{time_log:12}{prefix:12}{severity:12}{reset}{msg}\n'.format(
            color=self.severity_to_colors.get(severity),
            time_log='[{0}]'.format(
                datetime.datetime.utcnow().strftime('%H:%M:%S:%f')[:-3],
            ),
            prefix='[{0}]'.format(
                self.prefix.upper()[:10],
            ),
            severity='[{0}]'.format(
                severity.upper()[:10],
            ),
            msg=msg,
            reset=self.reset,
        )

    def create_child_logger(self, module_name: str):
        """
        Create child logger.

        Args:
            module_name (str): name of module for logging
        """
        self.child_logger = logging.getLogger(
            '{0}.{1}'.format(
                constants.PARENT_LOGGER_NAME,
                module_name,
            ),
        )
