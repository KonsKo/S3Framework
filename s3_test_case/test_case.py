"""
Test case module.

Unittest methods order:
- `TextTestRunner.run(discovered_tests: TestSuite)` - start process
- `TextTestRunner.run` invokes `discovered_tests.run` i.e. TestSuite.run()
- `TestSuite.run` invokes every `TestCase` (i.e. test class)
- Also, `TestSuite.run` invokes `setUpClass` method of `TestCase`
- Test case call is `TestCase.run`
- `TestCase.run` invokes `setUp` (`asyncSetUp`)

Process description:

TestCase aka test class is main/base unit. We prepare TestCase for running and clean it after use.
Server instance starts/stops (if it is possible) every TestCase.
All methods for preparing class in `class methods`.

!!! BEST WAY to reinit server - invoke `init_server`
!!! BEST WAY to reinit clients - invoke `init_clients`

Server instance is always being init, but it starts if it is required.
TestCase determines all actions for server and clients.

1. Default launch with tls, one server instance for TestCase:
    - default provided config with no changes
    - `s3_session` is True and `<>S3` server starts with tls
    - endpoint creates after server init, current configurations
    - clients init with created endpoint

2. Launch with config changing, one server instance for TestCase:
    - config data are changed directly in `setUpClass` before `super`

            >    def setUpClass(cls):
            >        cls.server_config.no_tls = True
            >        super().setUpClass()

    - `s3_session` is True and `<>S3` server starts with changed config
    - endpoint creates after server init, because config was changed

3. Launch with config changing, one server instance for every test:
    - invoke method `init_server` inside test with change config arguments.
    - start server
    - init method `init_clients` if you need some clients

            > self.init_server(log='s3_log_name.log')
            > self.server.start()
            > self.init_clients()

     - `s3_session` is False and `<>` server starts with changed config
     - endpoint creates after server init, because config was changed

4. Launch with real aws:
    - server config is not in use, because external server
    - `s3_session` is not in use and `ServerS3` instance is in work
    - endpoint creates after server init, based on real aws data
    - clients init with real aws endpoint

5. Launch with fs:
    - config for run server in container
    - `s3_session` is not in use and '<>S3<>FS' instance is in work
    - endpoint creates after server init, based on config data
    - clients init with created endpoint

"""
import asyncio
import hashlib
import io
import os
import sys
import time
from contextlib import contextmanager
from operator import itemgetter
from typing import Any, Literal, Optional, Union
from unittest import IsolatedAsyncioTestCase, skipIf, util

import constants
from clients.awsboto import Boto, BotoRealAws, BotoResponse
from clients.awscli import AwsCli
from helpers import cmd, exceptions, framework, output_handler, utils
from helpers.server_s3 import S3, S3FS, ServerS3, ServerS3Config

test_reaction = output_handler.OutputReaction(
    prefix='test',
    module_name=__name__,
)


def skip_if_root_user() -> skipIf:
    """
    Skip test(s) that CAN have SIDE effects if running from root user.

    May be applied for whole class.

    Returns:
        result (skipIf): result of test skipping

    """
    return skipIf(
        condition=framework.structure.run_as_root_user and not framework.structure.drop_cap,
        reason='Server is running as ROOT and drop capabilities was not set up.',
    )


def skip_if_external_server() -> skipIf:
    """
    Skip test(s) that CAN NOT run with <> FS or with AWS S3.

    May be applied for whole class.

    Returns:
        result (skipIf): result of test skipping

    """
    return skipIf(
        condition=framework.structure.is_external_server,
        reason='Test can be used only with internal <> S3 server.',
    )


def skip_if_internal_server() -> skipIf:
    """
    Skip test(s) that CAN NOT run with VFS.

    May be applied for whole class.

    Returns:
        result (skipIf): result of test skipping

    """
    return skipIf(
        condition=not framework.structure.is_external_server,
        reason='Test can be used only with <> FS.',
    )


def skip_if_not_installed(tool_name: str) -> skipIf:
    """
    Skip test(s) if `tool` was not installed.

    May be applied for whole class.

    Args:
        tool_name (str): tool/program/application name

    Returns:
        result (skipIf): result of test skipping

    """
    return skipIf(
        condition=not framework.structure.is_tool_installed(tool_name),
        reason='Required tool `{0}` was not installed.'.format(
            tool_name,
        ),
    )


class ListLen(object):
    """Counter for `assertDictContains`."""

    def __init__(self, element_count: int):
        """
        Init class instance.

        Args:
            element_count(int): element counter

        """
        self.element_count = element_count


class DictContains(dict):

    def __init__(self, **kwargs):
        self.update(kwargs)

    def __repr__(self):
        str_list = []
        for key, value in self.items():
            str_list.append('{}={}'.format(key, repr(value)))
        return 'DictContains({})'.format(', '.join(str_list))


class InDict(object):
    """Class to check key existence for assertDictContains."""


class S3AsyncTestCase(IsolatedAsyncioTestCase):
    """S3 Async Test case class representation."""

    constants = constants
    fw_instance = framework.structure

    config: dict

    # Separate server config for convenience.
    server_config: ServerS3Config

    # Auto-allocate class-scoped <>S3, AwsCli, and Boto instances.
    s3_session = True

    aws: Optional[AwsCli] = None
    boto: Optional[Boto] = None
    boto_second: Optional[Boto] = None
    boto_anonymous: Optional[Boto] = None

    server: Optional[ServerS3] = None

    endpoint_url = ''
    endpoint_host = ''

    attempts_to_re_set_up = 0

    # aliases, names without changes
    ListLen = ListLen
    DictContains = DictContains
    InDict = InDict

    # every class is being created at the discovery (by unittest)
    # method to copy config data - guaranty to have NO changed config for every TestCase
    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls)
        config = framework.structure.get_config_copy()
        obj.__class__.config = config
        obj.__class__.server_config = ServerS3Config(
            **config.get(constants.S3SERVER_PROC_NAME),
        )
        return obj

    def __init__(self, *args, **kwargs):
        """
        Init class instance.

        Args:
            args: extra arguments
            kwargs: extra key arguments
        """
        super().__init__(*args, **kwargs)
        self.class_errors = 0
        self.is_ignored = False
        self.description = ''

    async def asyncSetUp(self):
        """Set up before every test."""
        # Get rid of the Executing <Task pending name='Task-14'... message
        # https://bugs.python.org/issue38608
        asyncio.get_running_loop().set_debug(False)

        # if error in previous test - restart test case
        if framework.structure.test_had_error:
            test_reaction(
                msg='Test ERROR has been caught from previous test.',
                severity=constants.SEV_WARNING,
            )
            self.re_set_up_test_case()

    async def asyncTearDown(self):
        """Tear down after every test."""
        # for cases, if test fails
        self.fw_instance.enable_joining_server_logs()

        # try to sync appending server log to main log
        time.sleep(constants.SYNC_TIMEOUT * 3)

    @classmethod
    def setUpClass(cls):
        """
        Prepare TestCase class before start.

        It is entry point for every TestCase/TestClass.

        For every TestCase init new config copy.
        """
        test_reaction(
            msg='setUpClass for `{0}` is invoking'.format(cls.__name__),
            severity=cls.constants.SEV_INFO,
        )

        if cls.fw_instance.to_aws:
            cls.__init_server()
            cls.init_real_aws_clients()

        elif cls.fw_instance.to_fs:

            # we do not need start-stop for case with <> FS, because of very long starting
            if cls._main_server_is_running():
                cls.server = cls.fw_instance.main_server

                # endpoint should be created after server is created,
                # but before clients are created
                cls.endpoint_url = cls.server.create_endpoint_url()
                cls.endpoint_host = cls.server.create_endpoint_url(with_port=False)

            else:
                cls.__init_server()
                cls.server.start()
                cls.fw_instance.main_server = cls.server

            cls.init_clients()

        else:
            if cls.s3_session:
                cls.__init_server()
                cls.server.start()
                cls.init_clients()

            # we want to check for undefined process on working port, but not for case with aws
            else:
                try:
                    utils.check_port(
                        port=cls.server_config.listen_port,
                        force_stop=True,
                    )
                except Exception:
                    test_reaction(
                        msg='Failed to check port.',
                        severity=constants.SEV_CRITICAL,
                    )
                    raise

    @classmethod
    def tearDownClass(cls):
        """Tear down for class."""
        test_reaction(
            msg='tearDownClass for `{0}` is invoking'.format(cls.__name__),
            severity=cls.constants.SEV_INFO,
        )

        cls.fw_instance.test_had_error = False

        cls.fw_instance.clean_boto()

        # !!! always clean linked boto,
        # otherwise framework may take all memory and been killed by OOM
        if cls.server:
            cls.server.clean_linked_boto()
        if cls.fw_instance.main_server:
            cls.fw_instance.main_server.clean_linked_boto()

        # we do not need start-stop for case with <> FS, because of very long starting
        if not cls.fw_instance.to_fs:

            if cls.server:
                # <>s3.stop() automatically closes the linked boto connections
                cls.server.stop()

            cls.fw_instance.clean_s3()

        # always clean up, it is harmless
        cls.boto = None
        cls.boto_anonymous = None
        cls.boto_second = None
        cls.server = None

    @classmethod
    def re_set_up_test_case(cls):
        """Re-setup TestCase, i.e. invoke tearDownClass and setUpClass."""
        if cls.attempts_to_re_set_up <= 5:
            cls.attempts_to_re_set_up += 1
            test_reaction(
                msg='Started to re-setup TestCase. Attempt {0}.'.format(cls.attempts_to_re_set_up),
                severity=constants.SEV_WARNING,
            )
            cls.tearDownClass()
            cls.setUpClass()

        else:
            test_reaction(
                msg='Attempts to re-setup TestCase have been exceeded.',
                severity=constants.SEV_ERROR,
            )

    # full copy of `__init_server`,
    # allows to change class attribute from instance
    def init_server(self, no_tls: bool = False, **kwargs):
        """
        Init <>S3 server instance based on config.

        -- NOT START --

        Create endpoint after server init, because it depends on how server was started.

        Args:
            no_tls (bool): start server with no tls
            kwargs: any config fields
        """
        # check for running instances, terminated instances will be cleaned up at the tear down
        if self.__class__.server and self.__class__.server.is_running():

            raise test_reaction(
                msg='Init server: already running instance of server: {0}, \nClass : {1}.'.format(
                    self.__class__.server.process, self.__class__.__name__,
                ),
                severity=constants.SEV_ERROR,
                returned_exception=RuntimeError,
            )

        # method suppose to be invoked from particular test in TestCase
        # every test in TestCase wants to have NOT changed config
        self.__class__.server_config = ServerS3Config(
            **self.config.get(constants.S3SERVER_PROC_NAME),
        )

        if no_tls:
            self.__class__.server_config.no_tls = True
            self.__class__.server_config.tls_cert = None
            self.__class__.server_config.tls_key = None

        for kwarg_k, kwarg_v in kwargs.items():
            self.__class__.server_config.__dict__.update({kwarg_k: kwarg_v})

        # always create class instance
        if self.__class__.fw_instance.to_aws:
            self.__class__.server = ServerS3(config=self.server_config)

        elif self.__class__.fw_instance.to_fs:
            self.__class__.server = S3FS(config=self.server_config)

        else:
            self.__class__.server = S3(config=self.server_config)

        self.__class__.endpoint_url = self.__class__.server.create_endpoint_url()
        self.__class__.endpoint_host = self.__class__.server.create_endpoint_url(
            with_port=False,
        )

    # method is protected to have no any wishes to call it from anywhere
    @classmethod
    def __init_server(cls):
        """
        Init <>S3 server instance based on config.

        -- NOT START --

        Create endpoint after server init, because it depends on how server was started.
        """
        # always create class instance
        if cls.fw_instance.to_aws:
            cls.server = ServerS3(config=cls.server_config)

        elif cls.fw_instance.to_fs:
            cls.server = S3FS(config=cls.server_config)

        else:
            cls.server = S3(config=cls.server_config)

        cls.endpoint_url = cls.server.create_endpoint_url()
        cls.endpoint_host = cls.server.create_endpoint_url(with_port=False)

    @classmethod
    def init_clients(cls, ca_cert: Optional[str] = None):
        """
        Init all boto clients.

        Args:
            ca_cert (Optional[str]): client certificate

        """
        if not ca_cert:
            ca_cert = framework.structure.config.get('clients_ca_cert')

        try:
            cls.boto = Boto(
                ca_cert=ca_cert,
                profile_name=constants.SERVER_DEFAULT_PROFILE_NAME,
                endpoint_url=cls.endpoint_url,
            )
            cls.fw_instance.add_boto(cls.boto)
            cls.server.linked_boto.append(cls.boto)

            cls.boto_second = Boto(
                ca_cert=ca_cert,
                profile_name=constants.SERVER_SECOND_PROFILE_NAME,
                endpoint_url=cls.endpoint_url,
            )
            cls.fw_instance.add_boto(cls.boto_second)
            cls.server.linked_boto.append(cls.boto_second)

            cls.boto_anonymous = Boto(
                ca_cert=ca_cert,
                profile_name=constants.SERVER_ANONYMOUS_PROFILE_NAME,
                endpoint_url=cls.endpoint_url,
                unsigned=True,
            )
            cls.fw_instance.add_boto(cls.boto_anonymous)
            cls.server.linked_boto.append(cls.boto_anonymous)

            cls.aws = AwsCli(
                endpoint_url=cls.endpoint_url,
                ca_cert=ca_cert,
                profile_name=constants.SERVER_DEFAULT_PROFILE_NAME,
            )
        except exceptions.BotoInitError:
            sys.exit(1)

        except exceptions.LostConnectionError:
            test_reaction(
                msg='During `init clients` connection was lost. Try to reconnect.',
                severity=constants.SEV_INFO,
            )

    @classmethod
    def init_real_aws_clients(cls):
        """Create client with real AWS S3 credentials, set up client as main boto one."""
        test_reaction(
            msg="""\n\n
            ***************************************************

                WARNING!!!

                Requests will be made to real AWS S3 server.
                Real server can have LIMITS.

                <DO NOT> make many requests or requests with big data.

                Main test Boto client will be replaced to real AWS S3 client.

                If you wan to proceed - type `YES` bellow.

            ***************************************************
            """,
            severity=constants.SEV_WARNING,
        )

        confirmation = input('Type here:  ')
        if confirmation.lower() != 'yes':
            sys.exit(1)

        try:
            cls.boto = BotoRealAws(
                profile_name=constants.AWS_REAL_PROFILE_NAME,
            )
            cls.boto_second = BotoRealAws(
                profile_name=constants.AWS_SECOND_REAL_PROFILE_NAME,
            )
            cls.boto_anonymous = BotoRealAws(
                profile_name=constants.SERVER_ANONYMOUS_PROFILE_NAME,
                unsigned=True,
            )
            cls.aws = AwsCli(
                profile_name=constants.AWS_REAL_PROFILE_NAME,
            )

        except exceptions.BotoInitError:
            sys.exit(1)

    @classmethod
    def _main_server_is_running(cls) -> bool:
        """
        Check for main server instance exists and running.

        Returns:
            (bool): server state
        """
        state = bool(cls.fw_instance.main_server) and cls.fw_instance.main_server.is_running()

        test_reaction(
            msg='Main server state: {0} {1}'.format(
                state,
                cls.fw_instance.main_server.process if state else '',
            ),
            severity=constants.SEV_INFO,
        )

        return state

    async def t_head_bucket(
        self, bucket: str, return_code: int = constants.HTTP_STAT.OK, **kwargs,
    ) -> BotoResponse:
        """
        Head Bucket and check response.

        Using for check existence.

        Args:
            bucket (str): bucket name
            return_code (int): expected return http code
            kwargs: extra key attrs

        Returns:
            response (BotoResponse): result of request
        """
        response = await self.boto.head_bucket(
            Bucket=bucket, **kwargs,
        )

        self.assertHttpStatusEqual(
            response.status,
            return_code,
        )

        return response

    async def t_head_object(
        self, bucket: str, key: str, return_code: int = constants.HTTP_STAT.OK, **kwargs,
    ) -> BotoResponse:
        """
        Head Object and check response.

        Using for check existence.

        Args:
            bucket (str): bucket name
            key (str): object key name
            return_code (int): expected return http code
            kwargs: extra key attrs

        Returns:
            response (BotoResponse): result of request
        """
        response = await self.boto.head_object(
            Bucket=bucket, Key=key, **kwargs,
        )

        self.assertHttpStatusEqual(
            response.status,
            return_code,
        )

        return response

    async def t_create_bucket_via_boto(
        self,
        bucket: str,
        enable_versioning: bool = False,
        **kwargs,
    ) -> BotoResponse:
        """
        Test template: Create bucket via boto3.

        Args:
            bucket (str): bucket name
            enable_versioning (bool): flag to Enable bucket versioning
            kwargs: extra key attrs

        Returns:
            response (BotoResponse): result of request
        """
        # LocationConstraint is required in case request to real AWS S3
        if 'CreateBucketConfiguration' not in kwargs.keys():
            kwargs.update(
                {
                    'CreateBucketConfiguration': {
                        'LocationConstraint': constants.DEFAULT_REAL_AWS_LOCATION,
                    },
                },
            )

        resp_cb = await self.boto.create_bucket(
            Bucket=bucket,
            **kwargs,
        )

        # we always want to control bucket existence
        if resp_cb.status == self.constants.HTTP_STAT.CONFLICT:
            raise exceptions.BucketExistsError(resp_cb)

        self.assertHttpStatusEqual(
            resp_cb.status,
            self.constants.HTTP_STAT.OK,
        )

        await self.t_head_bucket(bucket=bucket)

        if enable_versioning:
            await self.t_put_bucket_versioning(bucket=bucket)

        return resp_cb

    async def t_put_bucket_versioning(
        self, bucket: str, status: Literal['Enabled', 'Suspended'] = 'Enabled',
    ):
        """
        Put Bucket Versioning.

        Args:
            bucket (str): bucket name
            status ( Literal['Enabled', 'Suspended']): VersioningConfiguration status

        """
        response_pbv = await self.boto.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={
                'Status': status,
            },
        )
        self.assertHttpStatusEqual(
            response_pbv.status,
            self.constants.HTTP_STAT.OK,
        )

    async def t_put_object_via_boto_with_content(
        self,
        bucket: str,
        key: str,
        content: Union[io.IOBase, str] = '',
        return_code: int = constants.HTTP_STAT.OK,
        **kwargs,
    ) -> BotoResponse:
        """
        Put object to bucket with ready content.

        Args:
            bucket (str): bucket name
            key (str): object key
            content (Union[io.IOBase, str]): content to upload
            return_code (int): expected http return code got assert
            kwargs: extra key arguments

        Returns:
            response (BotoResponse): result of request
        """
        if isinstance(content, str):
            content = content.encode()

        response = await self.boto.put_object(
            Bucket=bucket,
            Body=content,
            Key=key,
            **kwargs,
        )

        self.assertHttpStatusEqual(
            response.status,
            return_code,
        )

        return response

    async def t_put_bucket_policy(
        self,
        bucket: str,
        action: Union[str, list] = '*',
        resource: Union[str, list] = '*',
        *,
        effect: Literal['Allow', 'Deny'] = 'Allow',
        condition: Optional[Any] = None,
        principal: Optional[Any] = None,
        return_code: int = constants.HTTP_STAT.NO_CONTENT,
    ):
        """
        Create and Put BucketPolicy.

        Args:
            bucket (str): bucket name
            action (Union[str, list]): policy action
            resource (Union[str, list]): policy resource
            effect (Literal['Allow', 'Deny']): policy effect
            condition (Optional[Any]): policy condition
            principal (Optional[Any]): policy principal
            return_code (int): http return code

        Returns:
            response (BotoResponse): result of request
        """
        if not principal:
            principal = self.boto_second.owner_id

        bp = utils.BucketPolicy(
            profile=self.boto.profile,
            statement=[
                {
                    'sid': 'id-1',
                    'principal': principal,
                    'action': action,
                    'resource': resource,
                    'effect': effect,
                    'condition': condition if condition else None,
                },
            ],
        )

        resp_pbp = await self.boto.put_bucket_policy(
            shrunk_response=False,
            Bucket=bucket,
            Policy=bp.as_string_json(),
        )
        self.assertHttpStatusEqual(
            resp_pbp.status,
            return_code,
        )

        return resp_pbp

    async def t_get_object(
        self,
        bucket: str,
        key: str,
        return_code: int = constants.HTTP_STAT.OK,
        expected_body: str = None,
        multipart: bool = False,
        **kwargs,
    ) -> BotoResponse:
        """
        Get Object and check response.

        Args:
            bucket (str): bucket name
            key (str): object key name
            return_code (int): expected return http code
            expected_body (str): expected object body
            kwargs: extra key attrs

        Returns:
            response (BotoResponse): result of request
        """
        response = await self.boto.get_object(
            Bucket=bucket, Key=key, **kwargs,
        )

        self.assertHttpStatusEqual(
            response.status,
            return_code,
        )
        if expected_body is not None and response.get('Body'):
            body = response['Body'].read()
            self.assertEqual(
                body,
                expected_body.encode(),
                'File content has been corrupted',
            )
            if not multipart:
                self.assertEqual(
                    response['ETag'].replace('\"', ''),
                    hashlib.md5(body, usedforsecurity=False).hexdigest(),
                    'ETag value is incorrect',
                )

        return response

    def assertDictContains(self, actual_dict, extra_msg=None, /,  **expected_dict):
        """
        Check that values with matching keys in both actual_dict and expected_dict are equal.

        actual_dict may still contain any number of additional keys not mentioned in expected_dict.
        Similar to assertDictContainsSubset, but with recursion support.
        Can be very slow and memory hungry for large amounts of data in expected_dict,
        write your own optimized asserts for large data sets.
        """
        extra = ''
        if extra_msg is not None:
            extra = '\n{}'.format(extra_msg)
        self._assertDictContains(actual_dict, '', extra, expected_dict)

    def t_create_bucket_via_aws(
        self,
        bucket: str,
        aws_cli: Optional[AwsCli] = None,
    ):
        """
        Create bucket via aws cli and test.

        Reason: very often procedure

        Args:
            bucket (str): bucket name
            aws_cli (AwsCli): aws cli instance

        """
        if aws_cli:
            aws = aws_cli
        else:
            aws = self.aws

        aws.s3api.create_bucket(
            bucket=bucket,
            extra='--create-bucket-configuration LocationConstraint=us-west-1',
        )

    def assertLargeListEqual(
        self, actual_list: list, expected: list, keys: set[str], primary_key: str,
    ):
        """
        Compare a list of dicts "actual_list" with reference "expected".

        Generic version of ListPathsTestCase.comp_obj_lst().
        Extracts important keys from actual_list and compares two sorted lists.
        """
        assert isinstance(keys, set)
        assert isinstance(primary_key, str)
        assert isinstance(actual_list, list)

        self.assertEqual(
            len(actual_list),
            len(expected),
            'Lists length do not match',
        )

        sorted_expected = sorted(expected, key=itemgetter(primary_key))

        sorted_actuals = []
        for item in actual_list:
            self.assertIsInstance(item, dict)
            # item.keys() returns `set` hence we process `&` with `set` too
            self.assertEquals(
                item.keys() & {primary_key},
                {primary_key},
                'required keys are missing in the "{0}"'.format(item),
            )
            sorted_actuals.append({key: item[key] for key in keys})

        sorted_actuals.sort(key=itemgetter(primary_key))
        assert len(sorted_actuals) == len(actual_list)

        # Print assertion for the first element only
        for idx, _ in enumerate(expected):
            if sorted_expected[idx] != sorted_actuals[idx]:

                # Either missing element, excessive, or just inequal
                if sorted_actuals[idx + 1] == sorted_expected[idx]:
                    # sorted_actuals[idx] is excessive
                    self.fail(
                        '"{0}" item is excessive'.format(
                            sorted_actuals[idx],
                        ),
                    )

                elif sorted_actuals[idx] == sorted_expected[idx + 1]:
                    # sorted_actuals[idx] is a correct ite, but it should go next,
                    # so the correct item is missing.
                    self.fail(
                        'List is lacking "{0}" item'.format(
                            sorted_expected[idx],
                        ),
                    )

                else:
                    self.assertEqual(
                        sorted_expected[idx],
                        sorted_actuals[idx],
                    )

    def assertHttpStatusEqual(self, returned_status: int, expected_status: int):
        """
        Assert for equality of returned and expected HTTP status.

        Args:
            returned_status (int): returned http status
            expected_status (int): expected http status
        """
        return self.assertEqual(
            returned_status,
            expected_status,
            'Expected http status {0}.'.format(expected_status),
        )

    # override method to limit length of assertion error,
    # because it can take a lot of lines
    def assertIn(self, member, container, msg=None):
        """Just like self.assertTrue(a in b), but with a nicer default message."""
        if member not in container:
            standardMsg = '{0} not found in {1}'.format(
                util.safe_repr(member, short=True),
                util.safe_repr(container, short=True),
            )
            self.fail(self._formatMessage(msg, standardMsg))

    def get_effective_root(self) -> str:
        """
        Return path to real s3 root directory.

        Wrapper for `server.get_effective_root` to process tests if external server.

        Returns:
            s3root (str): real s3 root directory path
        """
        if self.fw_instance.is_external_server:
            self.skipTest(
                reason='Working with s3 root: it is allowed only for internal server run.',
            )

        if self.server:
            return self.server.get_effective_root()

    @contextmanager
    def set_up_immutable_attribute(self, target: str):
        """
        Set up immutable attribute to target.

        Context manager.

        Args:
            target (str): target(file or dir) to apply attribute

        Yields:
            None (None): Nothing, `yields` is required by decorator

        """
        if self.fw_instance.is_external_server:
            self.skipTest(
                reason='Changing file attributes: it is allowed only for internal server run.',
            )

        try:
            cmd.run_blocking(
                command_args=[
                    'sudo',
                    '-n',
                    'chattr',
                    '+i',
                    os.path.join(
                        self.get_effective_root(),
                        target,
                    ),
                ],
            )
            yield

        finally:
            cmd.run_blocking(
                command_args=[
                    'sudo',
                    '-n',
                    'chattr',
                    '-i',
                    os.path.join(
                        self.get_effective_root(),
                        target,
                    ),
                ],
            )

    # override method to catch assertion to process ignored tests
    def _callTestMethod(self, method):

        self._update_test_options()

        # if test is ignored - update num of runs
        # (`is_ignored` attribute set up by `self._update_test_options()`)
        if self.is_ignored:
            self.fw_instance.db.write_ignored_test(
                test_id=self.id(),
                num_of_runs_incr=0 if self.fw_instance.to_fs else 1,
                num_of_runs_fs_incr=1 if self.fw_instance.to_fs else 0,
            )

            test_reaction(
                msg='Test < {0} > is ignored: info is saved.'.format(self.id()),
                severity=constants.SEV_INFO,
            )

        try:
            self._callMaybeAsync(method)

        # Connection during the test - re-set up TestCase
        except exceptions.LostConnectionError as lt_err:
            # method is trying to re-set up class several times

            test_reaction(
                msg='During the test connection was lost.',
                severity=constants.SEV_INFO,
            )

            raise lt_err

        except AssertionError as assertion_error:

            # if test is ignored we, save it
            # (`is_ignored` attribute set up by `self._update_test_options()`)
            if self.is_ignored:
                self.fw_instance.db.write_ignored_test(
                    test_id=self.id(),
                    num_of_fails_incr=0 if self.fw_instance.to_fs else 1,
                    num_of_fails_fs_incr=1 if self.fw_instance.to_fs else 0,
                )

                test_reaction(
                    msg=[
                        'Test < {0} > is ignored: info is saved.'.format(self.id()),
                        'Assertion is failed: {0}'.format(assertion_error),
                    ],
                    severity=constants.SEV_WARNING,
                )

            else:
                raise assertion_error

    def _assertDictContains(self, actual_dict: dict, parent_key, extra_msg, expected_dict: dict):
        # When comparing lists, keys_found is used to determine the best match
        keys_found = 0
        for key, value in expected_dict.items():

            # check for key existence only
            if isinstance(value, InDict) and key in actual_dict.keys():
                keys_found += 1
                continue

            actual = actual_dict.get(key, None)
            keys_found += self._assertDictItemContains(actual, parent_key + key, extra_msg, value)

        return keys_found

    def _assertDictContainsStrict(self, actual_dict, parent_key, extra_msg, expected_dict):
        # When comparing lists, keys_found is used to determine the best match
        keys_found = 0
        for key, value in expected_dict.items():

            # check for key existence only
            if isinstance(value, InDict) and key in actual_dict.keys():
                keys_found += 1
                continue

            actual = actual_dict.get(key, None)
            keys_found += self._assertDictItemContains(actual, parent_key + key, extra_msg, value)

        missing_keys = set(actual_dict) - set(expected_dict)
        if missing_keys:
            self.fail('Excessive keys in "{}" {}{}'.format(parent_key, missing_keys, extra_msg))
        return keys_found

    def _assertDictItemContains(self, actual, parent_key, extra_msg, expected):
        if expected is None:
            self.assertIsNone(
                actual,
                '"{}" should not contain value{}'.format(parent_key, extra_msg),
            )
            return 1

        elif isinstance(expected, DictContains):
            self.assertIsInstance(
                actual, dict,
                '"{}" should be a dict{}'.format(parent_key, extra_msg),
            )
            return self._assertDictContains(actual, parent_key + '.', extra_msg, expected)

        elif isinstance(expected, dict):
            self.assertIsInstance(
                actual, dict,
                '"{}" should be a dict{}'.format(parent_key, extra_msg),
            )
            return self._assertDictContainsStrict(actual, parent_key + '.', extra_msg, expected)

        elif isinstance(expected, list):
            self.assertIsInstance(
                actual, list,
                '"{}" should be a list{}'.format(parent_key, extra_msg),
            )
            self.assertEqual(
                len(expected), len(actual),
                '"{}" list has unexpected length{}'.format(parent_key, extra_msg),
            )

            # O(len(expected)^2 * K * logK) in CPU and O(len(expected)^2) in RAM,
            # where K is key count in each item.
            # Matches only unique items.
            # Don't spoil our arguments, store the auxiliary data separately
            unmatched_actuals = bytearray(len(actual))
            matched_expecteds = [False for i in range(0, len(expected))]
            match_records = []

            for nested_idx, nested_expected in enumerate(expected):
                for actual_idx, actual_item in enumerate(actual):
                    try:
                        score = self._assertDictItemContains(
                            actual_item,
                            '{}[{}].'.format(parent_key, nested_idx),
                            extra_msg,
                            nested_expected,
                        )
                        match_records.append((score, nested_idx, actual_idx))
                    except self.failureException:
                        pass

            # sort by score, accept best matches first
            match_records.sort(key=itemgetter(0), reverse=True)

            for record in match_records:
                if not matched_expecteds[record[1]] and unmatched_actuals[record[2]] == 0:
                    unmatched_actuals[record[2]] = 1
                    matched_expecteds[record[1]] = True

            for expected_idx, match in enumerate(matched_expecteds):
                if not match:
                    self.fail(
                        '"{}" list should contain {} element'.format(
                            parent_key, repr(expected[expected_idx]),
                        ),
                    )

            # Return the highest score
            if len(match_records) > 0:
                return max(1, match_records[0][0])
            else:
                return 1

        elif type(expected) is type:
            self.assertIsNotNone(
                actual,
                '"{}" should be assigned{}'.format(parent_key, extra_msg),
            )
            self.assertIsInstance(
                actual, expected,
                '"{}" should be of type {}{}'.format(parent_key, expected.__name__, extra_msg),
            )
            return 1

        elif isinstance(expected, ListLen):
            self.assertIsNotNone(
                actual,
                '"{}" should be assigned{}'.format(parent_key, extra_msg),
            )
            self.assertEqual(
                expected.element_count,
                len(actual),
                '"{}" list has unexpected length{}'.format(parent_key, extra_msg),
            )
            return 1

        else:
            if actual is None:
                self.fail(
                    '"{}" value is missing, "{}" expected. {}'.format(
                        parent_key, expected, extra_msg,
                    ),
                )
            elif actual != expected:
                self.fail(
                    '"{}" value {} != {}{}'.format(
                        parent_key, repr(actual), repr(expected), extra_msg,
                    ),
                )

            return 1

    def _update_test_options(self):
        """
        Check and update test options.

        Set up `self.is_ignored` if test is ignored.
        Update list of ignored test with full-length id.

        """
        what_is_ignored = (
            self.id(),  # whole test path
            '{0}.{1}'.format(  # test class
                self.__class__.__module__,
                self.__class__.__qualname__,
            ),
            self.__class__.__module__,  # test module
        )

        for what in what_is_ignored:
            check = self.fw_instance.ignored_tests.get(what)
            if check:
                self.is_ignored = True
                self.description = check.get('reason', '')
                self.fw_instance.ignored_tests.update(test_id=self.id())
