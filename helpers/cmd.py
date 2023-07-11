"""
Cmd helper.

psutil.Popen is same as subprocess.Popen but
in addition it provides all psutil.Process methods in a single class.
"""
import shlex
import sys
from subprocess import PIPE
from subprocess import Popen as Popen_sp
from typing import Tuple, Union

from psutil import Popen as Popen_psutil

import constants
from helpers import exceptions, output_handler

cmd_reaction = output_handler.OutputReaction(
    prefix='CMD',
    module_name=__name__,
)

ERROR_START_PROC = 'Error starting process from command: {command}.'

# psutil Popen combines inside itself psutil.Popen, psutil.Process, subprocess.Pope
ProcessPopenType = Union[Popen_psutil, Popen_sp]

ResultT = Tuple[str, str, ProcessPopenType]


def get_stdout_encoding() -> str:
    """
    Get encoding.

    Returns:
        encoding (str): encoding
    """
    encoding = getattr(sys.__stdout__, 'encoding', None)

    if encoding is None:
        encoding = 'utf-8'

    return encoding


def cast_command_args(args_value: Union[str, list], with_shlex: bool = False) -> list:
    """
    Command args setter.

    Args:
        args_value (Union[str, list]): command arguments
        with_shlex (bool): use shlex module for lexical analysis

    Returns:
        args_value (list): list

    Raises:
        boto_reaction: if error happened
    """
    if isinstance(args_value, str):
        if with_shlex:
            return shlex.split(args_value)
        return args_value.split()

    elif isinstance(args_value, list):
        if with_shlex:
            return shlex.split(
                ' '.join(args_value),
            )
        return args_value

    raise cmd_reaction(
        msg='Wrong data type for arguments.',
        severity=constants.SEV_ERROR,
        returned_exception=TypeError,
    )


def run_nonblocking(
    command_args: Union[str, list],
    lexical_analysis: bool = False,
    *args,
    **kwargs,
) -> ProcessPopenType:
    """
    Create process, run it and do not block.

    Args:
        command_args (Union[str, list]): command line arguments
        lexical_analysis (bool): use shlex module for lexical analysis
        args: extra parameters
        kwargs: extra key parameters

    Returns:
        process (ProcessPopenType): Process instance

    Raises:
        boto_reaction: if error happened

    """
    try:
        return Popen_psutil(
            cast_command_args(command_args, with_shlex=lexical_analysis),
            *args,
            **kwargs,
        )

    except Exception as exception:
        raise cmd_reaction(
            msg=[
                ERROR_START_PROC.format(command=command_args),
                exception,
            ],
            severity=constants.SEV_EXCEPTION,
            returned_exception=exceptions.CreateCommandError,
        )


def run_blocking(
    command_args: Union[str, list],
    lexical_analysis: bool = False,
    expected_return_code: int = 0,
    *args,
    **kwargs,
) -> ResultT:
    """
    Create process, run it and block until process exit.

    Args:
        command_args (Union[str, list]): command line arguments
        lexical_analysis (bool): use shlex module for lexical analysis
        expected_return_code (int): expected return code, to NOT throw error for,
        args: extra parameters
        kwargs: extra key parameters

    Returns:
        out, err, process (Tuple[str, str, ProcessPopenType]): output, process

    Raises:
        boto_reaction: if error happened or return code does not equal to `expected_return_code`

    """
    try:
        process: ProcessPopenType = Popen_psutil(
            cast_command_args(command_args, with_shlex=lexical_analysis),
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
            *args,
            **kwargs,
        )

    except Exception as exception:
        raise cmd_reaction(
            msg=[
                ERROR_START_PROC.format(command=command_args),
                exception,
            ],
            severity=constants.SEV_EXCEPTION,
            returned_exception=exceptions.CreateCommandError,
        )

    try:
        out, err = process.communicate()

    except Exception as exception_com:
        raise cmd_reaction(
            msg=[
                'Error invoking a command: {0}.'.format(command_args),
                exception_com,
            ],
            severity=constants.SEV_EXCEPTION,
            returned_exception=exceptions.InvokingCommandError,
        )

    # Non-empty stderr output with zero (or some expected) exit code is
    # a correct exit. If some command fails with zero exit code,
    # then it should be handled in a separate function,
    # or via a special flag, or that's just a bug in <>s3.
    if int(process.returncode) != expected_return_code:
        raise cmd_reaction(
            msg='Process finished with return code `{0}`. Error: {1}.'.format(
                process.returncode,
                err.decode(get_stdout_encoding()).replace('\n', '') if err else 'no error message',
            ),
            severity=constants.SEV_EXCEPTION,
            returned_exception=exceptions.InvokingCommandError,
        )

    if not out:
        out = b''

    return out.decode(get_stdout_encoding()), err.decode(get_stdout_encoding()), process
