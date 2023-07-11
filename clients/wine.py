"""
Wine client module.

Wine should be installed and configured before running.

Possibly you should install `winetricks` to make configuring Wine easier.
"""
import os
from typing import Optional

from clients.base_client import BaseClient, ResultT
from helpers import output_handler, utils
from helpers.cli_cmd_maker import CommandMapperBase, Field

PROGRAM_FILES = '~/.wine/drive_c/Program Files'


class CommandWine(CommandMapperBase):
    """Wine command class."""

    main_command = 'wine'

    win_app: str = Field(
        option='',
        description='Path to Windows application..',
    )

    def normalize_win_app_path(cls, program_files: str = PROGRAM_FILES):
        """
        Normalize path for provided Windows application.

        Wine installed to `/home/USER/.wine` by default.
        Program installed to `/home/USER/.wine/drive_c/Program Files/` by default.

        Args:
            program_files (str): path to directory `Program files` inside `wine`.
                Override default behavior if needed.
        """
        cur_path = os.path.join(
            program_files,
            cls.win_app,
        )

        cls.win_app = utils.normalise_win_path(cur_path)


class CommandWineserver(CommandMapperBase):
    """
    Wineserver command class.

    Wineserver is a daemon process that provides to Wine roughly
    the same services that the Windows kernel provides on Windows.
    """

    main_command = 'wineserver'

    kill: Optional[bool] = Field(
        option='--kill',
        default=False,
        description='Kill the current wineserver.',
    )


class Wine(BaseClient):
    """Wine client class."""

    client_reaction = output_handler.OutputReaction(
        module_name=__name__,
        prefix='aws cli',
    )

    def start(
        self, app_path: str, program_files_path: Optional[str] = PROGRAM_FILES,
    ) -> ResultT:
        """
        Invoke Windows app.

        Args:
            app_path (str): path to Windows application.
            program_files_path (Optional[str]): path to directory `Program files` inside `wine`.

        Returns:
            result (ResultT): output, Process
        """
        kwargs = {'win_app': app_path}
        creator = CommandWine(**kwargs)
        creator.normalize_win_app_path(program_files=program_files_path)

        return self.invoke_command(
            creator.make_command(),
        )

    def stop(self) -> ResultT:
        """
        Kill the current wineserver.

        Returns:
            result (ResultT): output, Process
        """
        kwargs = {'kill': True}
        creator = CommandWineserver(
            **kwargs,
        )

        return self.invoke_command(
            creator.make_command(),
        )
