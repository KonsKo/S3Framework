"""Checker module."""
import json
import shutil
import sys
from typing import Optional

from pydantic import BaseModel

import constants
from helpers import cmd, output_handler

rch_reaction = output_handler.OutputReaction(
    module_name=__name__,
    prefix='check',
)

REQUIRED_MINOR = 8
REQUIRED_MAJOR = 3


class Tool(BaseModel):
    """Structure for tool."""

    name: str
    path: Optional[str] = ''
    required: Optional[bool] = False
    helper: Optional[str] = ''
    installed: bool = False

    def check(cls):
        """Check tool is installed by `name` or full `path`."""
        if bool(shutil.which(cls.name)) or bool(shutil.which(cls.path)):
            rch_reaction(
                msg='<{tool}> checked.'.format(
                    tool=cls.name,
                ),
                severity=constants.SEV_INFO,
            )
            cls.installed = True

        else:
            rch_reaction(
                msg='<{tool}> was not installed. Try: {help}'.format(
                    tool=cls.name,
                    help=cls.helper,
                ),
                severity=constants.SEV_CRITICAL,
            )
            cls.installed = False


def check_python_version():
    """Check python version."""
    version = sys.version_info
    if version.major == REQUIRED_MAJOR and version.minor >= REQUIRED_MINOR:
        rch_reaction(
            msg='Python version {0}.{1} is supported.'.format(
                version.major,
                version.minor,
            ),
            severity=constants.SEV_INFO,
        )
    else:
        rch_reaction(
            msg='Python version must be >= {0}.{1}. Current: {2}.{3}'.format(
                REQUIRED_MAJOR,
                REQUIRED_MINOR,
                version.major,
                version.minor,
            ),
            severity=constants.SEV_CRITICAL,
        )
        sys.exit(1)


def run_modprobe():
    """
    Run command 'modprobe tls' before tests.

    On some kernels the kTLS module must be loaded first.
    I met problem several times
    that tls das not works properly before that command invoking.
    """
    cmd.run_blocking(
        command_args='modprobe tls',
    )


def check_tools() -> list[Tool]:
    """
    Check for installed requirements, mark installed ones.

    Returns:
        tools (list[Tool]): list of requirements
    """
    check_python_version()

    run_modprobe()

    with open(constants.VERIFICATION_FILE_PATH) as vf:
        requirements = json.load(vf)

    tools = []

    for requirement in requirements:

        tool = Tool(**requirement)
        tool.check()

        if not tool.installed:
            if tool.required:
                sys.exit(1)

            rch_reaction(
                msg='<{tool}> is optional, keep action.Tests may fail.'.format(
                    tool=tool.name,
                ),
                severity=constants.SEV_WARNING,
            )

        tools.append(tool)

    return tools
