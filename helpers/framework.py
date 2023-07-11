"""Framework module."""
import copy
import os
import time
from threading import Event
from typing import TYPE_CHECKING, Optional

import constants
from clients.awsboto import Boto
from helpers.db import DB
from helpers.log_connector import connect_s3_server_log
from helpers.output_handler import OutputReaction
from helpers.requirements_checker import Tool, check_tools
from helpers.utils import check_client_config_file, remove_directory_content
from helpers.xml_errors_loader import ErrorsXML, load_xml_errors
from s3_test_case.disabler import load_broken_tests

# import for type hinting only by https://peps.python.org/pep-0484/
if TYPE_CHECKING:
    from helpers.server_s3 import ServerS3

fw_reaction = OutputReaction(
    prefix='framework',
    module_name=__name__,
)


def fill_up_xml_errors():
    """Load XML Errors and fill up related constant."""
    source_file_xml_errors = os.path.join(
        constants.S3SERVER_SOURCE_DIR,
        constants.XML_ERRORS_SOURCE_FILE_NAME,
    )
    xml_errors = load_xml_errors(source=source_file_xml_errors)
    constants.XML_ERRORS = ErrorsXML(**xml_errors)


class Framework(object):
    """Framework class."""

    def __init__(
        self, config: dict, to_aws: bool = False, to_fs: bool = False,
        drop_cap: bool = False, do_cleanup: bool = False,
    ):
        """
        Init class instance.

        Args:
            config (dict): framework config.
            to_aws (bool): is set up, requests will be made to real AWS S3 server
            to_fs (bool): is set up, framework will work with <> FS
            drop_cap (bool): remove capabilities from the prevailing bounding set for server
            do_cleanup (bool): clean files after work

        """
        self._config = config
        os.environ['LD_LIBRARY_PATH'] = constants.OPENSSL_LIBRARY_PATH
        self.check_clients_config_files()

        self.tools: list[Tool] = check_tools()

        fill_up_xml_errors()
        self.display_info = self.config.get('display_info', True)
        self.ignored_tests = load_broken_tests(source=constants.IGNOS3SERVER_TESTS_FILE_PATH)

        # we use main_server for running with <> fs to keep one instance and
        # do not to recreate-start-stop server every test case
        self.main_server: Optional['ServerS3'] = None

        self._s3_storage = {}
        self._boto_storage = set()
        self.test_had_error = False  # flag to handle test errors

        self.to_aws = to_aws
        self.to_fs = to_fs
        self.drop_cap = drop_cap
        self.run_as_root_user = False
        self.is_external_server = to_aws or to_fs
        self.do_cleanup = do_cleanup

        self.db = DB(config=self.config.get('db', {}))

        # allow to disable server log joining
        self._event_to_silent_joining_logs = None
        if self.config.get('join_server_log', False):
            self.join_s3_server_log_in_thread(
                log_file=self.config.get('<>s3')['log'],
            )

        if os.geteuid() == 0:
            self.run_as_root_user = True
            fw_reaction(
                msg='Server will run from ROOT user.',
                severity=constants.SEV_INFO,
            )

        fw_reaction(
            msg=[
                'Ready to work.',
                'TMP directory: {0}'.format(constants.TMP_DIR),
            ],
            severity=constants.SEV_INFO,
        )

    @property
    def config(self) -> dict:
        """
        Config.

        Returns:
            config (dict): config data

        Raises:
            NotImplementedError: if config does not exist
        """
        if self._config:
            return self._config

        raise fw_reaction(
            msg='Config does not exist.',
            severity=constants.SEV_CRITICAL,
            returned_exception=NotImplementedError,
        )

    def get_config_copy(self, part: Optional[str] = None) -> dict:
        """
        Get copy of config.

        Args:
            part (Optional[str]): part of config to return,
                example: get_config_copy(port='<>s3)
                    will return <>s3-related config part

        Returns:
            config_copy (dict): copy of config

        """
        if part:
            return copy.deepcopy(
                self.config.get(part),
            )

        return copy.deepcopy(self.config)

    def add_s3(self, s3):
        """
        Add <>s3 instance to storage.

        Args:
            s3: <> s3 instance

        """
        try:
            self._s3_storage[s3.process.pid] = s3
        except Exception:
            fw_reaction(
                msg='Failed to add <>s3 to storage: wrong type',
                severity=constants.SEV_ERROR,
            )
            return

        fw_reaction(
            msg='Added <> s3(pid={0}) to storage'.format(
                s3.process.pid,
            ),
            severity=constants.SEV_INFO,
        )

    def remove_s3(self, s3):
        """
        Remove <>s3 instance from storage.

        Args:
            s3: to remove

        """
        try:
            self._s3_storage[s3.process.pid] = None
        except Exception:
            fw_reaction(
                msg='Failed to remove <>s3 from storage',
                severity=constants.SEV_ERROR,
            )
            return

        fw_reaction(
            msg='Removed <> s3(pid={0}) from storage.'.format(
                s3.process.pid,
            ),
            severity=constants.SEV_INFO,
        )

    def clean_s3(self):
        """Stop and remove all <>s from storage."""
        server: 'ServerS3'
        if self._s3_storage:
            for server in self._s3_storage.values():
                if server:
                    server.stop()

            self._s3_storage = {}

    def add_boto(self, boto):
        """
        Register Boto instance.

        Args:
            boto: the instance

        Raises:
            KeyError: if already registered.
        """
        assert isinstance(boto, Boto)
        if boto in self._boto_storage:
            raise KeyError('Boto instance already registered')

        self._boto_storage.add(boto)

    def remove_boto(self, boto):
        """
        Unregister Boto instance without closing its connection.

        Args:
            boto: the instance
        """
        self._boto_storage.remove(boto)

    def clean_boto(self):
        """Close connections and clear the list of Boto instances."""
        while self._boto_storage:
            instance = self._boto_storage.pop()
            instance.close()

    def tear_down(self):
        """Clean up after framework was finished."""
        self.clean_boto()
        self.clean_s3()

        # TODO temporary disabled to have no side effects
        # self.db.clean_ignored_test(
        #    self.ignored_tests.get_tests_ids(),
        # )
        self.db.stop()

        self.clean_tmp()

        fw_reaction(
            msg=['Finished', 'TMP directory: {0}'.format(constants.TMP_DIR)],
            severity=constants.SEV_INFO,
        )

    def check_clients_config_files(self):
        """
        Check related clients (AWS, S#CMD) config files existence.

        We do checking if default file location was changed.
        """
        # mapping: config file field, venv name
        configs = (
            ('aws_config', constants.VENV_AWS_CONFIG_FILE),  # AWS
            ('s3cmd_config', None),  # S3CMD
        )
        for config in configs:

            if self.config.get(config[0]):  # if config path set up

                # check and set up client config file value as absolute path
                self.config[config[0]] = check_client_config_file(
                    config_file=self.config.get(config[0]),
                    venv=config[1],
                )

            else:
                fw_reaction(
                    msg='Config for {0} was not provided.'.format(config[0]),
                    severity=constants.SEV_INFO,
                )

    def join_s3_server_log_in_thread(self, log_file: str):
        """
        Join s3 server log and framework log.

        Args:
            log_file (str): server log file to jion
        """
        self._event_to_silent_joining_logs = Event()

        connect_s3_server_log(
            server_log_file=log_file,
            silent_event=self._event_to_silent_joining_logs,
        )

    def disable_joining_server_logs(self):
        """Stop to joining logs: server and framework."""
        if self._event_to_silent_joining_logs:
            self._event_to_silent_joining_logs.set()

    def enable_joining_server_logs(self):
        """Start back to joining logs: server and framework."""
        if self._event_to_silent_joining_logs:
            # small delaying to NOT allow disabled data are being printed out
            time.sleep(0.1)
            self._event_to_silent_joining_logs.clear()

    def clean_tmp(self):
        """Clean and remove tmp dir."""
        if (not self.is_external_server) and self.do_cleanup:
            remove_directory_content(
                constants.TMP_DIR,
            )

    def is_tool_installed(self, tool_name: str) -> bool:
        """
        Check tool is installed.

        Args:
            tool_name (str): tool/program/application name

        Returns:
            (bool): tool installation status
        """
        for tool in self.tools:
            if tool_name == tool.name:
                return tool.installed

        return False


structure: Optional[Framework] = None


def start_entity(
    config: dict,
    to_aws: bool = False,
    to_fs: bool = False,
    drop_cap: bool = False,
    do_cleanup: bool = False,
):
    """
    Start framework.

    Args:
        config (dict): framework config
        to_aws (bool): make requests to real AWS S3 server.
        to_fs (bool): framework will work with <> FS
        drop_cap (bool): remove capabilities from the prevailing bounding set for server
        do_cleanup (bool): clean files after work
    """
    global structure
    structure = Framework(
        config=config,
        to_aws=to_aws,
        to_fs=to_fs,
        drop_cap=drop_cap,
        do_cleanup=do_cleanup,
    )
