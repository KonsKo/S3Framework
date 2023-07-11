"""
WRK client module.

https://github.com/wg/wrk
"""
import re
from typing import Optional

import constants
from clients.base_client import BaseClient, ResultT
from helpers.cli_cmd_maker import CommandMapperBase, Field
from helpers.output_handler import OutputReaction


class WrkCMD(CommandMapperBase):
    """WRK client mapping of command line arguments."""

    main_command = 'wrk'

    connections: Optional[int] = Field(option='-c', default=None)
    duration: Optional[int] = Field(option='-d', default=None)
    threads: Optional[int] = Field(option='-t', default=None)
    header: Optional[str] = Field(option='-H', default=None)
    timeout: Optional[int] = Field(option='--timeout', default=None)
    latency: bool = Field(
        option='--latency',
        default=False,
        description='Print detailed latency statistics.',
    )
    script: Optional[str] = Field(
        option='-s',
        default=None,
        description='Path to LuaJIT script.',
    )
    url: str = Field(description='Target url.')


class Wrk(BaseClient):
    """Wrk client class."""

    client_reaction = OutputReaction(
        module_name=__name__,
        prefix='wrk',
    )

    def __init__(self):
        super().__init__()
        self.http_method = 'GET'  # used only for printing out the info

    def run(self, **kwargs) -> ResultT:
        """
        Run WRK benchmark.

        Args:
            kwargs: command options

        Returns:
            result (ResultT): output, Process
        """
        creator = WrkCMD(**kwargs)

        if creator.script:
            self.parse_http_method_from_script(creator.script)

        out, proc = self.invoke_command(creator.make_command())

        self.client_reaction(
            msg='Benchmark response, http method: {0}'.format(
                self.http_method,
            ),
            severity=constants.SEV_INFO if out else constants.SEV_ERROR,
        )

        return out, proc

    def parse_http_method_from_script(self, script_file: str):
        """
        Parse wrk script file.

        *** NOW only http method is returned. ***

        We want to catch method to print it out ONLY, because
        default wrk message did not contain such information.

        Args:
            script_file (str): wrk script file

        """
        with open(script_file) as sf:
            script_data = sf.read()

        script_data = script_data.split('\n')
        for line in script_data:
            line = line.strip()
            if line.startswith('wrk.method'):
                # Raw line `wrk.method = "PUT"`
                self.http_method = re.search(r'\"(\D+)\"', line).group(1)

    def parse_response(self, wrk_response: str) -> int:
        """
        Parse WRK response.

        *** NOW only Requests/sec is returned. ***

        Args:
            wrk_response (str): response from WRK

        Returns:
            Requests/sec (int): number of request per second
        """
        wrk_response = wrk_response.split('\n')
        for line in wrk_response:
            line = line.strip()
            if line.startswith('Requests/sec:'):
                # Raw line `Requests/sec:  15112.00`
                return int(re.search(r'\d+', line).group())


wrk = Wrk()
