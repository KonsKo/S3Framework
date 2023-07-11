"""
Boto3 client.

Boto3 as AWS CLI uses config file: /home/<username>/.aws/credentials

Required fields for non-anonymous requests:

File filling example:

    [default]
    parameter_validation = false
    aws_access_key_id = JL76AF8XQI6KTJ48V9
    aws_secret_access_key = d6mq7l3mD9qKhQlehJ6N0y4hqAMoyOu5I0XaroN2

    s3 =
        signature_version = s3v4


    [profile second_user]
    parameter_validation = false
    aws_access_key_id = 6MIWAK68KV7CX4E8K71B
    aws_secret_access_key = PbPmSG8ko9O8vVZIZZEnmoRpdIZMq0vmN50IkGEU

    s3 =
        signature_version = s3v4

Initialisations in this module are cheap:
- boto3.Session() is about 6 ms according `timeit`
- boto3.Session() + boto3.Session().client() is about 27 ms according `timeit`

"""
import asyncio
import contextvars
import io
import weakref
from functools import partial, wraps
from typing import TYPE_CHECKING, Callable, Optional

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError, ProfileNotFound
from botocore.response import StreamingBody

import constants
from helpers import exceptions, output_handler

# import for type hinting only by https://peps.python.org/pep-0484/
if TYPE_CHECKING:
    from helpers.server_s3 import ServerS3

boto_reaction = output_handler.OutputReaction(
    prefix='boto3',
    module_name=__name__,
)


def async_wrap(func: Callable) -> Callable:
    """
    Run a synchronous function as async task in a thread.

    Decorator. Decorated function should be thread-safe.

    Args:
        func (Callable): sync callable to make async

    Returns:
        func (Callable): async callable func

    """

    @wraps(func)
    async def _run_async(*args, **kwargs):
        """
        Run sync function as async.

        Full copy of asyncio.to_tread()
        Reason not use `to_thread`: capability with bython 3.8

        Args:
            args: extra arguments
            kwargs: extra key arguments

        Returns:
            result of async function

        Raises:
            boto_reaction: if error happened

        """
        # exact copy of asyncio.to_thread
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        func_call = partial(ctx.run, func, *args, **kwargs)
        return await loop.run_in_executor(None, func_call)

    return _run_async


class BotoResponse(dict):
    status: int
    headers: dict
    owner: Optional[dict]
    error: Optional[dict]
    errors: Optional[list]
    metadata: Optional[dict]
    body: Optional[StreamingBody]
    grants: Optional[list]
    version_id: Optional[str]
    etag: Optional[str]

    author: Optional[str]
    author_id: Optional[str]
    author_name: Optional[str]
    version_id: Optional[str]


class Boto(object):
    """Class for implementing boto3 client."""

    silent = False

    not_existed_profiles = (
        constants.RED_ANONYMOUS_PROFILE_NAME,
        constants.RED_NOT_EXISTENT_PROFILE_NAME,
    )

    cached_profiles = {}

    SERVICE_NAME = 's3'
    PLACEHOLDER_SHRUNK = '(shrunk)'
    MSG_LENGTH = 300

    def __init__(
        self,
        endpoint_url: str = None,
        ca_cert: Optional[str] = None,
        unsigned: bool = False,
        profile_name: str = constants.RED_DEFAULT_PROFILE_NAME,
        service_name: Optional[str] = None,
    ):
        """
        Init class instance.

        Args:
            endpoint_url (str): RED S3 endpoint url
            ca_cert (Optional[str]): ssl certificate file
            unsigned (bool): if True requests will be anonymous
            profile_name (str): settings profile name
            service_name (Optional[str]): service name to create session

        """
        self.service_name = service_name or self.SERVICE_NAME
        self.ca_cert = ca_cert
        self.profile = profile_name
        self.owner_id = ''
        self.owner_name = ''
        self.aws_access_key_id = ''
        self.aws_secret_access_key = ''
        self.owner_details = {}
        self.body_refs = []

        self.session = self.create_session(
            profile_name=None if unsigned else profile_name,
        )

        if self.session:
            self.client = self._create_client(endpoint_url=endpoint_url, unsigned=unsigned)
            self.unsigned = unsigned

            # load data if service is s3
            if self.service_name == self.SERVICE_NAME:
                self.load_profile_data()

        self.invoke_method = async_wrap(self.invoke_method_sync)

    def __getattr__(self, attr):
        """
        Get attr, validate it and run command if validation was ok.

        It works as alias. You can call Boto.create_bucket(Bucket='name')
            and if botocore.client.S3 has method `create_bucket`
            it calls that method

        Args:
            attr : attribute

        Returns:
            result of `invoke_command`

        Raises:
            boto_reaction: if error happened

        """

        def default(**kwargs):  # noqa:WPS430
            return self.invoke_method(
                method=attr,
                **kwargs,
            )

        if attr in constants.EXISTED_COMMANDS_AWS_BOTO:
            return default

        raise boto_reaction(
            msg='Unsupported command name: <{0}>.'.format(attr),
            severity=constants.SEV_ERROR,
            returned_exception=exceptions.UnsupportedCommandNameError,
        )

    def create_session(self, profile_name: str) -> boto3.Session:
        """
        Create boto session.

        Args:
            profile_name (str): profile name to create session

        Returns:
            session (boto3.Session): boto session object
        """
        try:
            return boto3.Session(
                profile_name=profile_name,
            )

        # if profile name does not exist
        except ProfileNotFound:
            self.session = None
            raise boto_reaction(
                msg='Profile `{0}` is not found.'.format(profile_name),
                severity=constants.SEV_CRITICAL,
                returned_exception=exceptions.BotoInitError,
            )

    def close(self):
        """Close boto connection and referenced ones."""
        old_refs = self.body_refs
        self.body_refs = []
        for ref in old_refs:
            body = ref()
            if body:
                body.close()

        # body's connection is released into self.client pool, so we should close it last
        # if `session` does not exist `client` does not exist too
        if self.session:
            self.client.close()

    def invoke_method_sync(
        self, method: str, shrunk_response: bool = True, **kwargs,
    ) -> BotoResponse:
        """
        Invoke botocore.client.S3 method.

        Args:
            method (str): method name
            shrunk_response (bool): full or not full response
            kwargs: extra key arguments

        Returns:
            response (BotoResponse) response of method invoking

        Raises:
            boto_reaction: if error happened

        """
        try:
            method_to_call = getattr(self.client, method)
        except Exception as exception:
            raise boto_reaction(
                msg=[
                    'BOTO3 wrong command name: <{0}>.'.format(method),
                    exception,
                ],
                severity=constants.SEV_EXCEPTION,
                returned_exception=AttributeError,
            )

        # limit msg length because it can be too long
        msg = 'To: {endpoint} - <{method}> with profile <{user_profile}>: {parameters}'.format(
            endpoint=self.client.meta.endpoint_url,
            method=method,
            parameters=kwargs,
            user_profile=self.profile,
        )
        if not self.silent:
            self._print_out_reaction(
                msg=str(msg), shrunk_response=shrunk_response,
            )

        try:
            response = method_to_call(**kwargs)

        # these exceptions are related to Connection, it is sign to connection is lost
        except BotoCoreError as boto_core_error:
            raise boto_reaction(
                msg=['Possibly, connection was lost.', boto_core_error],
                severity=constants.SEV_EXCEPTION,
                returned_exception=exceptions.LostConnectionError,
            )

        except ClientError as client_error:
            return self._parse_response(client_error.response, shrunk_response=shrunk_response)

        except Exception as exception_resp:
            raise boto_reaction(
                msg=[
                    'Error invoking command: <{0}>.'.format(method),
                    exception_resp,
                ],
                severity=constants.SEV_EXCEPTION,
                returned_exception=RuntimeError,
            )

        return self._parse_response(response, shrunk_response=shrunk_response)

    def load_profile_data(self):
        """Load profile-related data: id and email."""
        if self.profile in self.not_existed_profiles:
            self.owner_id = constants.RED_ANONYMOUS_PROFILE_NAME
            self.owner_name = constants.RED_ANONYMOUS_PROFILE_NAME

        else:
            if self.profile in self.cached_profiles:
                cached_profile_data = self.cached_profiles.get(self.profile, {})
                self.owner_id = cached_profile_data.get('owner_id')
                self.owner_name = cached_profile_data.get('owner_name')
                self.aws_access_key_id = cached_profile_data.get('aws_access_key_id')
                self.aws_secret_access_key = cached_profile_data.get('aws_secret_access_key')
                return

            response = self.invoke_method_sync(
                method='list_buckets',
            )
            try:
                self.owner_id = response.owner.get('ID')
                self.owner_name = response.owner.get('DisplayName')
                self.aws_access_key_id = self.session.get_credentials().access_key
                self.aws_secret_access_key = self.session.get_credentials().secret_key
                self.cached_profiles[self.profile] = {
                    'owner_id': self.owner_id,
                    'owner_name': self.owner_name,
                    'aws_access_key_id': self.aws_access_key_id,
                    'aws_secret_access_key': self.aws_secret_access_key,
                }

            except AttributeError:
                if response and response.status != constants.HTTP_STAT.OK:
                    boto_reaction(
                        msg=[
                            'Failed to load data for profile: `{0}`.'.format(self.profile),
                            response.get('Error', {}).get('Message', ''),
                        ],
                        severity=constants.SEV_CRITICAL,
                        returned_exception=exceptions.BotoInitError,
                    )

    def _create_client(self, endpoint_url: Optional[str] = None, unsigned: bool = False):
        """
        Create boto3 client instance.

        Args:
            endpoint_url ( Optional[str]): The complete URL to use for the constructed client.
                Normally, botocore will automatically construct the URL.

            unsigned (bool): if True, requests will be anonymous

        Returns:
            client (botocore.client.S3): client instance

        Raises:
            boto_reaction: if error happened
        """
        try:
            config = Config(connect_timeout=10, read_timeout=100, retries={'max_attempts': 0})
            if unsigned:
                config = config.merge(Config(signature_version=UNSIGNED))

            return self.session.client(
                verify=self.ca_cert if self.ca_cert else None,
                service_name=self.service_name,
                endpoint_url=endpoint_url,
                config=config,
            )

        except Exception as exception:
            raise boto_reaction(
                msg=['Failed to create client instance.', exception],
                severity=constants.SEV_EXCEPTION,
                returned_exception=NotImplementedError,
            )

    def _parse_response(self, response, shrunk_response: bool = True) -> BotoResponse:
        if not self.silent:
            self._print_out_reaction(
                msg='Raw response: {0}'.format(str(response)), shrunk_response=shrunk_response,
            )

        if response:
            # if response['Body'] is a stream, then it prevents its connection from closing.
            if 'Body' in response and isinstance(response['Body'], io.IOBase):
                self.body_refs.append(weakref.ref(response['Body']))

            try:
                # boto3 operation returns a dictionary with unique structure for each operation,
                # ResponseMetadata being the only common key, and Error is always present
                # for all error responses.
                response = BotoResponse(response)

            except Exception as exception:
                raise boto_reaction(
                    msg=['Failed to parse response.', exception],
                    severity=constants.SEV_EXCEPTION,
                    returned_exception=NotImplementedError,
                )

            response.status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            response.headers = response.get('ResponseMetadata', {}).get('HTTPHeaders')
            response.owner = response.get('Owner')
            response.error = response.get('Error')
            response.errors = response.get('Errors')
            response.metadata = response.get('Metadata')
            response.body = response.get('Body')
            response.grants = response.get('Grants')
            response.version_id = response.get('VersionId')
            response.etag = response.get('ETag')

            # remember that Error.Code isn't always equal to ResponseMetadata.HTTPStatusCode
            response['HTTPStatusCode'] = response.get('ResponseMetadata', {}).get('HTTPStatusCode')

            response['ErrorCode'] = response.get('Error', {}).get('Code')
            response['ErrorMessage'] = response.get('Error', {}).get('Message')

            response.author = self.profile
            response.author_id = self.owner_id
            response.author_name = self.owner_name

            return response

    def _print_out_reaction(self, msg: str, shrunk_response: bool = True):
        # if flag, msg will cut by length otherwise full
        response_msg = str(msg)[:self.MSG_LENGTH] if shrunk_response else str(msg)
        boto_reaction(
            msg='{1}{0}'.format(
                response_msg,
                self.PLACEHOLDER_SHRUNK if len(response_msg) < len(str(msg)) else '',
            ),
            severity=constants.SEV_INFO,
        )


class BotoRealAws(Boto):
    """Boto client to process requests to real AWS S3."""

    def load_profile_data(self):
        """Load real AWS profile data."""
        super().load_profile_data()

        if not self.unsigned:
            # real AWS allow to get user identity with Security Token Service
            inner_boto = BotoRealAws(
                profile_name=self.profile, service_name='sts',
            )
            self.owner_details = inner_boto.invoke_method_sync(
                method='get_caller_identity',
            )
            inner_boto.close()


class AllBoto(object):
    """Class to process of invoking methods for all boto clients."""

    def __init__(self, server: 'ServerS3'):
        """
        Init class instance.

        Args:
            server (ServerS3): res4 server instance
        """
        self.server: 'ServerS3' = server

    def __getattr__(self, attr: str, *args, **kwargs) -> Callable:
        """
        Get class attribute to process invoking.

        Args:
            attr (str): attribute to call
            args: extra arguments
            kwargs: extra key arguments

        Returns:
            callable (Callable): class method to call
        """
        self.method: str = attr
        return self.start_tasks

    async def start_tasks(self, *args, **kwargs) -> tuple:
        """
        Create and start tasks.

        Tasks - invoking on method (self.method) for every boto instance

        Args:
            args: extra arguments
            kwargs: extra key arguments

        Returns:
            results (tuple): results of tasks
        """
        tasks = []

        if self.server:
            if self.server.linked_boto:
                for boto in self.server.linked_boto:
                    task = boto.invoke_method(
                        method=self.method, **kwargs,
                    )
                    tasks.append(task)

                return await asyncio.gather(*tasks)

    def silent_on(self):
        if self.server:
            if self.server.linked_boto:
                for boto in self.server.linked_boto:
                    boto.silent = True

    def silent_off(self):
        if self.server:
            if self.server.linked_boto:
                for boto in self.server.linked_boto:
                    boto.silent = False
