"""
Warp client module.

https://github.com/minio/warp

Warp put `--debug` output to stderr.
"""
import re
from typing import Optional

import constants
from clients.base_client import BaseClient, ResultT
from helpers import output_handler
from helpers.cli_cmd_maker import CommandMapperBase, Field


class BaseWarpCMD(CommandMapperBase):
    """Base class to do mapping of command line arguments."""

    main_command = 'warp'

    attr_divider = '='

    host: str = Field(option='--host')
    access_key: str = Field(option='--access-key')
    secret_key: str = Field(option='--secret-key')
    tls: bool = Field(option='--tls', default=True)
    insecure: bool = Field(option='--insecure', default=True)
    duration: str = Field(option='--duration', default='1m')
    obj_randsize: bool = Field(
        option='--obj.randsize',
        default=False,
        description='Randomize object sizes.',
    )
    bucket: Optional[str] = Field(
        option='--bucket',
        default=None,
        description='Change default bucket.',
    )
    no_clear: Optional[bool] = Field(
        option='--noclear',
        default=False,
        description='Do not delete client data.',
    )
    concurrent: Optional[int] = Field(
        option='--concurrent',
        description='Tweak concurrency.',
    )
    object_size: Optional[str] = Field(
        option='--obj.size',
        description='The size of objects to upload with suffix of unit.',
    )
    debug: Optional[bool] = Field(
        option='--debug',
        default=False,
        description='Debug output (is processed as stderr).',
    )


class WarpAnalyzeCMD(CommandMapperBase):
    """Mapping for command `analyze`."""

    main_command = 'warp'
    inner_command = 'analyze'

    filename: str = Field(description='Saved data/file name.')


class WarpMultipartCMD(BaseWarpCMD):
    """Mapping for command `multipart`."""

    inner_command = 'multipart'

    parts: int = Field(option='--parts')
    part_size: str = Field(option='--part.size')


class WarpMixedCMD(BaseWarpCMD):
    """Mapping for command `mixed`(`get`, 'put', ...)."""

    inner_command = 'mixed'


class WarpGetCMD(WarpMixedCMD):
    """Mapping for command `get`."""

    inner_command = 'get'

    objects: Optional[int] = Field(option='--objects')


class Warp(BaseClient):
    """Warp client class."""

    client_reaction = output_handler.OutputReaction(
        module_name=__name__,
        prefix='warp',
    )

    def report_processing(self, command_result: str, debug_data: Optional[str] = None):
        """
        Find and analyze the warp report.

        Args:
            command_result (str): warp command result to grab report name from.
            debug_data (Optional[str]): add debug response if applicable

        """
        report_file = warp.find_warp_report_file(command_result=command_result)

        if report_file:
            self.client_reaction(
                msg='Last report file is found: <{0}>. Analyzing report.'.format(report_file),
                severity=constants.SEV_INFO,
            )

            report_analysis, _, _ = self.analyze(
                filename=report_file,
            )
            self.client_reaction(
                msg='Report analysis:\n{0}'.format(report_analysis),
                severity=constants.SEV_INFO,
            )

            if debug_data:
                report_file_debug = re.sub(r'-(\D+)-', '-debug-', report_file)
                report_file_debug = report_file_debug.replace('csv.zst', 'txt')
                self._process_debug_output(
                    debug_data=debug_data,
                    file_name=report_file_debug,
                )

        else:
            self.client_reaction(
                msg='Could not found report file.',
                severity=constants.SEV_WARNING,
            )

    def multipart(self, **kwargs) -> ResultT:
        """
        Run Warp 'multipart' and generate report in the end of work.

        Args:
            kwargs:command options

        Returns:
            (ResultT): result of command, err, process
        """
        creator = WarpMultipartCMD(**kwargs)

        out, err, proc = self.invoke_command(
            command=creator.make_command(),
        )

        # warp puts debug data to stderr
        self.report_processing(
            command_result=out,
            debug_data=err,
        )

        return out, err, proc

    def mixed(self, **kwargs) -> ResultT:
        """
        Run Warp 'mixed' and generate report in the end of work.

        Args:
            kwargs:command options

        Returns:
            (ResultT): result of command, err, process
        """
        creator = WarpMixedCMD(**kwargs)

        out, err, proc = self.invoke_command(
            command=creator.make_command(),
        )

        # warp consider debug data as stderr
        self.report_processing(
            command_result=out,
            debug_data=err if creator.debug and err else None,
        )

        return out, err, proc

    def get(self, **kwargs) -> ResultT:
        """
        Run Warp 'get' and generate report in the end of work.

        Args:
            kwargs:command options

        Returns:
            (ResultT): result of command, err, process
        """
        creator = WarpGetCMD(**kwargs)

        out, err, proc = self.invoke_command(
            command=creator.make_command(),
        )

        # warp consider debug data as stderr
        self.report_processing(
            command_result=out,
            debug_data=err if creator.debug and err else None,
        )

        return out, err, proc

    def analyze(self, **kwargs) -> ResultT:
        """
        Run Warp 'analyze'.

        Args:
            kwargs:command options

        Returns:
            (ResultT): result of command, err, process
        """
        creator = WarpAnalyzeCMD(**kwargs)

        return self.invoke_command(
            command=creator.make_command(),
        )

    @staticmethod
    def find_warp_report_file(command_result: str) -> str:
        """
        Find report file for warp.

        After work is done, warp print out file name with report to console.
        Example: warp: Benchmark data written to "warp-mixed-2022-10-20[203556]-WP0O.csv.zst"

        Args:
            command_result (str): warp output

        Returns:
            name (str): report file name
        """
        return re.search(
            pattern=r'\"(.+\.csv\.zst)\"',
            string=command_result,
        ).group(1)

    def _process_debug_output(self, debug_data: str, file_name: str):
        """
        Write debug putput to file.

        Args:
            debug_data (str): debug data to write to file
            file_name (str): file to save data to
        """
        self.client_reaction(
            msg='Warp is running with `--debug` option. Result will be saved to `{0}`'.format(
                file_name,
            ),
            severity=constants.SEV_INFO,
        )

        try:
            with open(file_name, 'a+') as rp:
                rp.writelines(debug_data)
        except Exception as exc:
            self.client_reaction(
                msg=['Failed to write out debug info', exc],
                severity=constants.SEV_EXCEPTION,
            )


warp = Warp()
