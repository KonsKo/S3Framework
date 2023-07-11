"""
AWS Cli module.

AWS CLI uses config file: /home/<username>/.aws/credentials

Required fields for non-anonymous requests:

File filling example:

    [default]
    aws_access_key_id = <value>
    aws_secret_access_key = <value>

"""
from typing import Literal, Optional

import constants
from clients.base_client import BaseClient, ResultT
from helpers import exceptions, output_handler
from helpers.cli_cmd_maker import CommandMapperBase, Field


# noinspection PyTypeChecker
class AwsCli(object):
    """AWS CLI Class implementation."""

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        ca_cert: Optional[str] = None,
        profile_name: str = constants.RED_DEFAULT_PROFILE_NAME,
    ):
        """
        Init class instance.

        Args:
            endpoint_url (Optional[str]): RED S3 endpoint url
            ca_cert (Optional[str]): ssl certificate file
            profile_name (str): settings profile name

        """
        super().__init__()
        self.endpoint_url = endpoint_url
        self.ca_cert = ca_cert
        self.profile_name = profile_name
        self.global_options = {
            'endpoint_url': self.endpoint_url,
            'profile': self.profile_name,
            'ca_bundle': self.ca_cert,
        }
        self.s3 = ServiceS3(global_options=self.global_options)
        self.s3api = ServiceS3API(global_options=self.global_options)


class AWSCommandBase(CommandMapperBase):
    """Base AWS CLI command class with global attributes."""

    main_command = 'aws'
    command_group: Literal['s3', 's3api'] = Field(description='Service name', default='s3')
    inner_command: str = Field(description='Inner command name depending of `service`')

    # global
    debug: Optional[bool] = Field(
        option='--debug',
        default=False,
        glob=True,
        description='Turn on debug logging.',
    )
    endpoint_url: Optional[str] = Field(
        option='--endpoint-url',
        glob=True,
        description='Override command’s default URL with the given URL.',
    )
    no_verify_ssl: Optional[bool] = Field(
        option='--no-verify-ssl',
        glob=True,
        description='This option overrides the default behavior of verifying SSL certificates.',
    )
    no_paginate: Optional[bool] = Field(
        option='--no-paginate',
        default=False,
        glob=True,
        description='Disable automatic pagination.',
    )
    output: Literal['json', 'text', 'table', 'yaml', 'yaml-stream'] = Field(
        option='--output',
        default='json',
        glob=True,
        description='The formatting style for command output.',
    )
    query: Optional[str] = Field(
        option='--query',
        default=None,
        glob=True,
        description='A JMESPath query to use in filtering the response data.',
    )
    profile: str = Field(
        option='--profile',
        glob=True,
        description='Use a specific profile from your credential file.',
    )
    region: Optional[str] = Field(
        option='--region',
        default=None,
        glob=True,
        description='The region to use. Overrides config/env settings.',
    )
    version: Optional[str] = Field(
        option='--version',
        default=None,
        glob=True,
        description='Display the version of this tool',
    )
    no_sign_request: Optional[bool] = Field(
        option='--no-sign-request',
        default=False,
        glob=True,
        description='Do not sign requests. Credentials will not be loaded.',
    )
    ca_bundle: Optional[str] = Field(
        option='--ca-bundle',
        default=None,
        glob=True,
        description='The CA certificate bundle to use when verifying SSL certificates',
    )
    cli_read_timeout: Optional[int] = Field(
        option='--cli-read-timeout',
        default=None,
        glob=True,
        description='The maximum socket read time in seconds.',
    )
    cli_connect_timeout: Optional[int] = Field(
        option='--cli-connect-timeout',
        default=None,
        glob=True,
        description='The maximum socket connect time in seconds. ',
    )


class CommandS3apiBase(AWSCommandBase):
    """
    Class for `s3api` service commands.

    Commands for service did not divided in separate class.
    """

    command_group: Literal['s3api'] = 's3api'

    bucket: Optional[str] = Field(
        option='--bucket',
        default=None,
        description='The name of the bucket.',
    )
    key: Optional[str] = Field(
        option='--key',
        default=None,
        description='Object key.',
    )
    body: Optional[str] = Field(
        option='--body',
        default=None,
        description='Object data.',
    )
    part_number: Optional[str] = Field(
        option='--part-number',
        default=None,
        description='Part number of part being uploaded.',
    )
    upload_id: Optional[str] = Field(
        option='--upload-id',
        default=None,
        description='Upload ID identifying the multipart upload.',
    )
    delimiter: Optional[str] = Field(
        option='--delimiter',
        default=None,
        description='A delimiter is a character you use to group keys.',
    )
    copy_source: Optional[str] = Field(
        option='--copy-source',
        default=None,
        description='Specifies the source object for the copy operation.',
    )

    extra: Optional[str] = Field(
        option='',
        default=None,
        description='Any extra argument',
    )


class CommandS3Base(AWSCommandBase):
    """Base class for `s3` service commands."""

    command_group: Literal['s3'] = 's3'


class CommandS3presign(CommandS3Base):
    """Class for particular `s3` service command: `presign`."""

    inner_command = 'presign'

    path: str = Field(
        option='',
        description='<S3Uri>',
    )
    expires_in: Optional[int] = Field(
        option='--expires-in',
        description='Number of seconds until the pre-signed URL expires.',
    )


class CommandS3cp(CommandS3Base):
    """Class for particular `s3` service command: `cp`."""

    inner_command = 'cp'

    path_source: str = Field(
        option='',
        description='<S3Uri> <LocalPath>',
    )
    path_destination: str = Field(
        option='',
        description='<S3Uri> <LocalPath>',
    )
    acl: Optional[str] = Field(
        option='--acl',
        default=None,
        description='Sets the ACL for the object when the command is performed.',
    )
    grants: Optional[str] = Field(
        option='--grants',
        default=None,
        description='Grant specific permissions to individual users or groups',
    )


class CommandS3ls(CommandS3Base):
    """Class for particular `s3` service command: `ls`."""

    inner_command = 'ls'

    path: Optional[str] = Field(
        option='',
        default=None,
        description='<S3Uri>',
    )
    recursive: Optional[bool] = Field(
        option='--recursive',
        default=False,
        description='Performed on all files or objects under the specified directory or prefix.',
    )
    page_size: Optional[int] = Field(
        option='--page-size',
        default=None,
        description='The number of results to return in each response to a list operation.',
    )


class CommandS3mb(CommandS3Base):
    """Class for particular `s3` service command: `ls`."""

    inner_command = 'mb'

    path: str = Field(
        option='',
        description='<S3Uri>',
    )


class CommandS3mv(CommandS3cp):
    """Class for particular `s3` service command: `mv`."""

    inner_command = 'mv'


class CommandS3rb(CommandS3Base):
    """Class for particular `s3` service command: `rb`."""

    inner_command = 'rb'

    path: str = Field(
        option='',
        description='<S3Uri>',
    )
    force: Optional[bool] = Field(
        option='--force',
        default=False,
        description='Deletes all objects in the bucket including the bucket itself.',
    )


class CommandS3rm(CommandS3Base):
    """Class for particular `s3` service command: `rm`."""

    inner_command = 'rm'

    path: str = Field(
        option='',
        description='<S3Uri>',
    )
    recursive: Optional[bool] = Field(
        option='--recursive',
        default=False,
        description='All files or objects under the specified directory or prefix..',
    )


class CommandS3sync(CommandS3cp):
    """Class for particular `s3` service command: `sync`."""

    inner_command = 'sync'


class CommandS3website(CommandS3Base):
    """Class for particular `s3` service command: `website`."""

    inner_command = 'website'

    path: str = Field(
        option='',
        description='<S3Uri>',
    )


class ServiceBase(BaseClient):
    """Base class for aws service commands."""

    service = None
    client_reaction = output_handler.OutputReaction(
        module_name=__name__,
        prefix='aws cli',
    )

    def __init__(self, global_options: dict = None):
        super().__init__()
        if not self.service:
            raise AttributeError(
                'Attr `service` must be set up.',
            )
        self.global_options = global_options if global_options else {}


class ServiceS3(ServiceBase):
    """
    Class for aws s3 service commands.

    All commands are declared explicit as methods to have IDE hinting.
    """

    service = 's3'

    def cp(self, **kwargs) -> ResultT:
        """
        Invoke `aws s3 cp` command.

        Copies a local file or S3 object to another location locally or in S3.

        Args:
            kwargs: command options, all available options are listed in mapping class

        Returns:
            result (ResultT): output, Process

        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3cp(**kwargs)
        return self.invoke_command(
            command=creator.make_command(),
        )

    def ls(self, **kwargs) -> ResultT:
        """
        Invoke `aws s3 ls` command.

        List S3 objects and common prefixes under a prefix or all S3 buckets.

        Args:
            kwargs: command options, all available options are listed in mapping class

        Returns:
            result (ResultT): output, Process

        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3ls(**kwargs)
        return self.invoke_command(
            command=creator.make_command(),
        )

    def mb(self, **kwargs) -> ResultT:
        """
        Invoke `aws s3 mb` command.

        Creates an S3 bucket.

        Args:
            kwargs: command options, all available options are listed in mapping class

        Returns:
            result (ResultT): output, Process

        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3mb(**kwargs)
        return self.invoke_command(
            command=creator.make_command(),
        )

    def mv(self, **kwargs) -> ResultT:
        """
        Invoke `aws s3 mv` command.

        Moves a local file or S3 object to another location locally or in S3.

        Args:
            kwargs: command options, all available options are listed in mapping class

        Returns:
            result (ResultT): output, Process

        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3mv(**kwargs)
        return self.invoke_command(
            command=creator.make_command(),
        )

    def presign(self, **kwargs) -> ResultT:
        """
        Invoke `aws s3 presign` command.

        Generate a pre-signed URL for an Amazon S3 object.

        Args:
            kwargs: command options, all available options are listed in mapping class

        Returns:
            result (ResultT): output, Process

        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3presign(**kwargs)
        return self.invoke_command(
            command=creator.make_command(),
        )

    def rb(self, **kwargs) -> ResultT:
        """
        Invoke `aws s3 rb` command.

        Deletes an empty S3 bucket.

        Args:
            kwargs: command options, all available options are listed in mapping class

        Returns:
            result (ResultT): output, Process

        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3rb(**kwargs)
        return self.invoke_command(
            command=creator.make_command(),
        )

    def rm(self, **kwargs) -> ResultT:
        """
        Invoke `aws s3 rb` command.

        Deletes an S3 object.

        Args:
            kwargs: command options, all available options are listed in mapping class

        Returns:
            result (ResultT): output, Process

        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3rm(**kwargs)
        return self.invoke_command(
            command=creator.make_command(),
        )

    def sync(self, **kwargs) -> ResultT:
        """
        Invoke `aws s3 rb` command.

        Syncs directories and S3 prefixes.

        Args:
            kwargs: command options, all available options are listed in mapping class

        Returns:
            result (ResultT): output, Process

        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3sync(**kwargs)
        return self.invoke_command(
            command=creator.make_command(),
        )

    def website(self, **kwargs) -> ResultT:
        """
        Invoke `aws s3 rb` command.

        Set the website configuration for a bucket.

        *** Are not implemented in RED S3 for now ***

        Args:
            kwargs: command options, all available options are listed in mapping class

        Returns:
            result (ResultT): output, Process

        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3website(**kwargs)
        return self.invoke_command(
            command=creator.make_command(),
        )


class ServiceS3API(ServiceBase):
    """
    Class for aws s3api commands.

    All commands are NOT declared explicit as methods.
    """

    service = 's3api'

    def __getattr__(self, attr):
        """
        Get attr, validate it and run command if validation was ok.

        Args:
            attr : attribute

        Returns:
            result (ResultT): output, Process

        Raises:
            boto_reaction: if error happened

        """
        def default(**kwargs) -> ResultT:
            kwargs = {
                'inner_command': attr.replace('_', '-'),
                **self.global_options,
                **kwargs,
            }
            cmd_creator = CommandS3apiBase(**kwargs)
            return self.invoke_command(
                command=cmd_creator.make_command(),
            )

        if attr in constants.EXISTED_COMMANDS_AWS_CLI_S3API:
            return default

        raise self.client_reaction(
            msg='Unsupported command name: <{0}>.'.format(attr),
            severity=constants.SEV_ERROR,
            returned_exception=exceptions.UnsupportedCommandNameError,
        )
