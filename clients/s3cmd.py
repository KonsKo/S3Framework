"""
S3CMD module.

S#CMD uses config file: /home/<username>/.s3cfg

https://s3tools.org/usage
"""
from typing import Optional

from pydantic import Field, root_validator

from clients.base_client import BaseClient, ResultT
from helpers import cli_cmd_maker, framework, output_handler


class S3CMDCommandBase(cli_cmd_maker.CommandMapperBase):
    """Base AWS CLI command class with global attributes."""

    main_command = 's3cmd'

    attr_divider = '='

    # global
    host: Optional[str] = Field(
        option='--host',
        glob=True,
        description='S3 endpoint. You should also set --host-bucket.',
    )
    host_bucket: Optional[str] = Field(
        option='--host-bucket',
        glob=True,
        description='S3 endpoint. You should also set --host-bucket.',
    )
    config: Optional[str] = Field(
        option='--config',
        glob=True,
        description='Config file name.',
    )
    ca_cert: Optional[str] = Field(
        option='--ca-certs',
        glob=True,
        description='Path to SSL CA certificate file.',
    )
    access_key: Optional[str] = Field(
        option='--access_key',
        glob=True,
        description='AWS Access Key.',
    )
    secret_key: Optional[str] = Field(
        option='--secret_key',
        glob=True,
        description='AWS Secret Key.',
    )

    # inner
    bucket: Optional[str] = Field(
        description='Bucket name',
        exclude=True,
    )
    key: Optional[str] = Field(
        description='Object key name.',
        exclude=True,
    )
    body: Optional[str] = Field(
        description='Path to file.',
    )
    # provided bucket and key values will be combined in one field
    bucket_key: Optional[str] = Field(
        description='Bucket value + object key value, i.e. `s3://BUCKET/KEY`.',
    )
    extra: Optional[str] = Field(
        description='Any extra parameter.',
    )

    @root_validator
    def validate_bucket_and_key(cls, values: dict) -> dict:
        """
        Validate bucket value and(or) object key value, by the docs.

        Returns:
            values (dict): updated values
        """
        bucket_v = values.get('bucket')
        key_v = values.get('key')
        if bucket_v:
            values['bucket_key'] = 's3://{bucket}{div}{key}'.format(
                bucket=bucket_v,
                div='/' if key_v else '',
                key=key_v if key_v else '',
            )

        return values


class CommandS3CMDCreateBucket(S3CMDCommandBase):
    """S3cmd create bucket command."""

    inner_command = 'mb'


class CommandS3CMDDeleteBucket(S3CMDCommandBase):
    """S3cmd delete bucket command."""

    inner_command = 'rb'


class CommandS3CMDListBucketsOrObjects(S3CMDCommandBase):
    """S3cmd list buckets or objects command."""

    inner_command = 'ls'


class CommandS3CMDPutObject(S3CMDCommandBase):
    """S3cmd put object command."""

    inner_command = 'put'


class CommandS3CMDGetObject(S3CMDCommandBase):
    """S3cmd put object command."""

    inner_command = 'get'

    @root_validator
    def correct_order_of_arguments(cls, values: dict) -> dict:
        """
        Change order of arguments.

        Returns:
            values (dict): updated values
        """
        # argument `body` should be in the end, by the docs,
        # changing order of class elements does bot change order of elements inside dict
        body_v = values.pop('body')
        values.update({'body': body_v})

        return values


class CommandS3CMDDeleteObject(S3CMDCommandBase):
    """S3cmd delete object command."""

    inner_command = 'del'


class CommandS3CMDCopyObject(S3CMDCommandBase):
    """S3cmd delete object command."""

    inner_command = 'cp'

    bucket_dest: str = Field(
        description='Destination bucket name',
        exclude=True,
    )
    key_dest: str = Field(
        description='Destination object key name.',
        exclude=True,
    )
    # provided bucket and key values will be combined in one field
    bucket_key_dest: Optional[str] = Field(
        description='Destination bucket value + object key value, i.e. `s3://BUCKET/KEY`.',
    )

    @root_validator
    def validate_dest_bucket_and_key(cls, values: dict) -> dict:
        """
        Validate destination bucket value and(or) object key value, by the docs.

        Returns:
            values (dict): updated values
        """
        values['bucket_key_dest'] = 's3://{bucket}/{key}'.format(
            bucket=values.get('bucket_dest'),
            key=values.get('key_dest'),
        )

        return values


class S3Cmd(BaseClient):
    """S3CMD class implementation."""

    client_reaction = output_handler.OutputReaction(
        module_name=__name__,
        prefix='s3cmd',
    )

    def __init__(
        self,
        endpoint_url: str,
        ca_cert: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
    ):
        """
        Init class instance.

        Args:
            endpoint_url (str): RED S3 endpoint url
            ca_cert (Optional[str]): ssl certificate file
            access_key (Optional[str]): account access key, default value is from config file
            secret_key (Optional[str]): account secret Key, default value is from config file

        """
        super().__init__()
        self.endpoint_url = endpoint_url
        self.config_path = framework.structure.get_config_copy(part='s3cmd_config') or None
        self.ca_cert = ca_cert
        self.access_key = access_key
        self.secret_key = secret_key
        self.global_options = {
            'host': self.endpoint_url,
            'host_bucket': self.endpoint_url,
            'config': self.config_path,
            'ca_cert': self.ca_cert,
            'access_key': self.access_key,
            'secret_key': self.secret_key,
        }

    def create_bucket(self, **kwargs) -> ResultT:
        """
        Create bucket.

        Args:
            kwargs: command options

        Returns:
            result (ResultT): output, Process
        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }

        creator = CommandS3CMDCreateBucket(**kwargs)

        return self.invoke_command(
            command=creator.make_command(),
        )

    def delete_bucket(self, **kwargs) -> ResultT:
        """
        Delete bucket.

        Args:
            kwargs: command options

        Returns:
            result (ResultT): output, Process
        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3CMDDeleteBucket(**kwargs)

        return self.invoke_command(
            command=creator.make_command(),
        )

    def list_buckets(self, **kwargs) -> ResultT:
        """
        List buckets.

        Args:
            kwargs: command options

        Returns:
            result (ResultT): output, Process
        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3CMDListBucketsOrObjects(**kwargs)

        return self.invoke_command(
            command=creator.make_command(),
        )

    def list_objects(self, **kwargs) -> ResultT:
        """
        List object (same as list buckets by the docs).

        Args:
            kwargs: command options

        Returns:
            result (ResultT): output, Process
        """
        return self.list_buckets(**kwargs)

    def put_object(self, **kwargs) -> ResultT:
        """
        Put object.

        Args:
            kwargs: command options

        Returns:
            result (ResultT): output, Process
        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3CMDPutObject(**kwargs)

        return self.invoke_command(
            command=creator.make_command(),
        )

    def get_object(self, **kwargs) -> ResultT:
        """
        Get object.

        Args:
            kwargs: command options

        Returns:
            result (ResultT): output, Process
        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3CMDGetObject(**kwargs)

        return self.invoke_command(
            command=creator.make_command(),
        )

    def delete_object(self, **kwargs) -> ResultT:
        """
        Delete object.

        Args:
            kwargs: command options

        Returns:
            result (ResultT): output, Process
        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3CMDDeleteObject(**kwargs)

        return self.invoke_command(
            command=creator.make_command(),
        )

    def copy_object(self, **kwargs) -> ResultT:
        """
        Copy object.

        Args:
            kwargs: command options

        Returns:
            result (ResultT): output, Process
        """
        kwargs = {
            **self.global_options,
            **kwargs,
        }
        creator = CommandS3CMDCopyObject(**kwargs)

        return self.invoke_command(
            command=creator.make_command(),
        )
