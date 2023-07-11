"""Utilities module, based on psutil."""
import asyncio
import hashlib
import inspect
import io
import json
import os
import random
import shutil
import signal
from codecs import encode
from contextlib import contextmanager
from copy import deepcopy
from typing import Any, Coroutine, List, Literal, Optional, Union

import psutil
import xlwt
from pydantic import BaseModel, Extra, Field, validator

import constants
from helpers import cmd, output_handler

STATUS_ZOMBIE = 'zombie'
START = 1
FINISH = 10000

utils_reaction = output_handler.OutputReaction(
    module_name=__name__,
    prefix='utils',
)


class DummyStream(io.IOBase):
    max_chunk_size = 4096

    def __init__(self, size=0, byte=None):
        self._size = size.__index__()
        self._chunk_size = min(self.max_chunk_size, self._size)

        if byte:
            assert isinstance(byte, (bytes, bytearray, str))
            if isinstance(byte, str):
                byte = bytes(byte, 'utf-8')
            else:
                byte = byte[:1]
            self._buf = byte * self._chunk_size
        else:
            self._buf = bytes(self._chunk_size)

        self._pos = 0

    def read(self, size=-1):
        if size is None:
            size = -1
        if size < 0:
            size = self._size

        bytecount = size.__index__()
        if bytecount + self._pos > self._size:
            bytecount = max(0, self._size - self._pos)
        if bytecount > self._chunk_size:
            bytecount = self._chunk_size

        self._pos += bytecount
        return self._buf[:bytecount]

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._pos

    def seek(self, pos, whence=0):
        if whence == 0:
            if pos < 0:
                raise ValueError('negative seek position {}'.format(pos))
            self._pos = pos
        elif whence == 1:
            self._pos = max(0, self._pos + pos)
        elif whence == 2:
            self._pos = max(0, self._size + pos)
        else:
            raise ValueError('unsupported whence value')

        return self._pos


def find_processes_by_name(
    proc_name: str, ignore_statuses: Optional[set] = (STATUS_ZOMBIE,),
) -> List[psutil.Process]:
    """
    Find all processes by name.

    Args:
        proc_name (str): process name to try to find
        ignore_statuses (Optional[set]): list of ignored statuses

    Returns:
        processes (List[psutil.Process]): list of found processes
    """
    if not ignore_statuses:
        ignore_statuses = ()

    found_processes = []
    for proc in psutil.process_iter():
        pinfo = proc.as_dict(attrs=['pid', 'name', 'status', 'create_time'])

        if proc_name == pinfo['name'] and pinfo['status'] not in ignore_statuses:
            found_processes.append(proc)

    return found_processes


def find_process_by_pid(
    pid: int, ignore_statuses: Optional[set] = (STATUS_ZOMBIE,),
) -> Optional[psutil.Process]:
    """
    Find processes by pid.

    Args:
        pid (int): process name to try to find
        ignore_statuses (Optional[set]): list of ignored statuses

    Returns:
        process (Optional[psutil.Process]): found process
    """
    if not ignore_statuses:
        ignore_statuses = ()

    try:
        proc = psutil.Process(pid)
    except Exception:
        return None

    if proc:
        if proc.status() not in ignore_statuses:
            return proc

        else:
            utils_reaction(
                msg='Process {0} is ignored by status'.format(str(proc)),
                severity=constants.SEV_WARNING,
            )


def run_async_as_sync(async_func: Coroutine):
    """
    Run async function as sync function.

    Args:
        async_func (Coroutine): async function to call

    Returns:
        result of sync invoking async_func
    """
    return asyncio.run(
        async_func,
    )


def generate_random_word(extra: Any = '') -> str:
    """
    Generate random word in deterministic way.

    Deterministication may be adjusted with command line option.

    File with words should be places next to this file.

    Args:
        extra (Any): allow to generate different values for invoking in one function

    Returns:
        word (str): random word
    """
    extra = str(extra)

    lineno = str(inspect.stack()[1].lineno)
    function = inspect.stack()[1].function
    filename = os.path.basename(inspect.stack()[1].filename)
    random.seed(filename + function + lineno + extra + constants.SEED_RANDOM_EXTRA)
    words_path = os.path.join(
        os.path.dirname(__file__),
        constants.WORDS_FILE_NAME,
    )
    if os.path.exists(words_path):
        with open(words_path, 'r') as wrf:
            words = wrf.read()

    else:
        utils_reaction(
            msg='File with words does not exists.',
            severity=constants.SEV_WARNING,
        )
        words = ['no_words']

    return '{0}{1}'.format(
        random.choice(words.splitlines()),  # noqa:S311
        random.randint(START, FINISH),  # noqa:S311
    )


def md5_of_stream(stream):
    md5_sum = hashlib.md5()  # noqa:S324
    while True:
        chunk = stream.read(4096)
        if not chunk:
            break
        md5_sum.update(chunk)

    return md5_sum.hexdigest()


def check_port(
    port: Union[str, int], force_stop: bool = False,
) -> Optional[psutil._common.sconn]:
    """
    Check port state.

    Return data about existing listeners for this port,
    optionally terminate the listening process with "force_stop" option.

    Args:
        port (Union[str, int]): port to check
        force_stop (bool): stop port-related process

    Returns:
        busy_conn (Optional[psutil._common.sconn]): port related conn data
    """
    busy_conn = None
    connections = psutil.net_connections(kind='inet4')
    for conn in connections:
        if conn.laddr and conn.laddr.port == int(port) and conn.pid:
            busy_conn = conn
            break

    if busy_conn:
        utils_reaction(
            msg='Port {0} is busy: {1}. '.format(
                port,
                busy_conn,
            ),
            severity=constants.SEV_WARNING,
        )

        if force_stop:
            stop_process_by_pid(busy_conn.pid)

    return busy_conn


def stop_process_by_pid(pid: int):
    """
    Stop process by pid.

    Should be used for undefined process (process we do not know about).

    Args:
        pid (int): pid of process to stop
    """
    proc = find_process_by_pid(pid)

    if proc:
        utils_reaction(
            msg='Trying to stop process {0}.'.format(
                proc,
            ),
            severity=constants.SEV_INFO,
        )

        proc.send_signal(signal.SIGINT)

        try:
            proc.wait(timeout=constants.S3SERVER_KILL_TIMEOUT)
        except Exception:
            proc.kill()

        utils_reaction(
            msg='Process with PID {0} has been stopped.'.format(
                pid,
            ),
            severity=constants.SEV_INFO,
        )

    else:
        raise utils_reaction(
            msg='Process with PID {0} does not exist.'.format(
                pid,
            ),
            severity=constants.SEV_INFO,
            returned_exception=ValueError,
        )


BOUNDARY = 'test_boundary'


class PostPayloadCreator(object):
    """Class to implement creation of POST request payload."""

    tagging_mapping = '<Tagging><TagSet>{tagging}</TagSet></Tagging>'
    tag_set_mapping = '<Tag><Key>{tag_key}</Key><Value>{tag_val}</Value></Tag>'

    def __init__(self, boundary: str = BOUNDARY):
        """
        Init class instance.

        Args:
            boundary (str): boundary

        """
        self.boundary_encoded = encode('--{0}'.format(boundary))
        self.boundary = boundary
        self.headers = {
            'Content-type': 'multipart/form-data; boundary={}'.format(self.boundary),
        }

    def make_post_payload(
        self,
        parameters: Optional[dict] = None,
        filename: str = 'test_file_name.txt',
        content: Optional[Union[str, bytes]] = None,
        exclude_content: bool = False,
        **kwargs,
    ):
        r"""
        Make POST request payload.

        Example of payload structure:

            --{boundary}\r\n
            Content-Disposition: form-data; name=key;\r\n
            Content-Type: text/plain\r\n
            \r\n
            {key}\r\n
            --{boundary}\r\n
            Content-Disposition: form-data; name={parameters.key};\r\n
            Content-Type: text/plain\r\n
            \r\n
            {parameters.value}\r\n
            --{boundary}\r\n
            Content-Disposition: form-data; name=acl;\r\n
            Content-Type: text/plain\r\n
            \r\n
            {acl}\r\n
            --{boundary}\r\n
            Content-Disposition: form-data; name=file; filename={filename}\r\n
            Content-Type: text/plain\r\n
            \r\n
            {content}
            \r\n
            --{boundary}\r\n

        Args:
            filename (str): filename to put in request
            parameters (Optional[dict]): request parameters, i.e. {'x-amz-meta-user': 'username'}
            content (Optional[Union[str, bytes]]): content to process request with
            exclude_content (bool): flag to not include content to payload
            kwargs (dict): extra key parameters

        Returns:
            payload (bytes): POST payload
        """
        payload_data = [
            self.boundary_encoded,
        ]

        if not parameters:
            parameters = {}

        # You can set up parameters via kwargs such `key='value'`
        # `lower` is used to do unification for key arguments, for example:
        # for POST form we should use argument `key` with lower `k` otherwise form error,
        # but for boto we got used to use `Key` with upper `K`, and it allows to us use
        # ether `key` or 'Key' as key argument with same result.
        if kwargs:
            parameters.update(
                {kwarg_k.lower(): kwarg_v for kwarg_k, kwarg_v in kwargs.items()},
            )

        for par_k, par_v in parameters.items():

            payload_data.extend(
                self._add_payload_part(
                    part_name=par_k, part_value=par_v,
                ),
            )

        if not exclude_content:
            payload_data.extend(
                self._add_content(filename=filename, content=content),
            )

        payload_data.append(encode(''))

        return b'\r\n'.join(payload_data)

    @classmethod
    def tag_set_to_xml(cls, tag_set: list[dict]) -> str:
        """
        Build TagSet XML from dict.

        <Tagging>
          <TagSet>
            <Tag>
              <Key>TagName</Key>
              <Value>TagValue</Value>
            </Tag>
            ...
          </TagSet>
        </Tagging>

        Args:
            tag_set(list[dict]): tag set to build from dict

        Returns:
            tag_set(str): tag set as XML
        """
        tagging = ''

        for ts in tag_set:
            tagging += cls.tag_set_mapping.format(tag_key=ts.get('Key'), tag_val=ts.get('Value'))

        return cls.tagging_mapping.format(tagging=tagging)

    def _add_payload_part(self, part_name: str, part_value: str):
        return [
            encode('Content-Disposition: form-data; name="{0}";'.format(part_name)),
            encode('Content-Type: text/plain'),
            encode(''),
            encode(part_value),
            self.boundary_encoded,
        ]

    def _add_content(self, filename: str, content: Optional[Union[str, bytes]] = ''):

        if isinstance(content, str):
            content = encode(content)

        return [
            encode(
                'Content-Disposition: form-data; name="file"; filename="{0}"'.format(filename),
            ),
            encode('Content-Type: application/octet-stream'),
            encode(''),
            content,
            encode('--{0}--'.format(self.boundary)),
        ]


post_payload_creator = PostPayloadCreator()


class DictDuplicateKey(object):
    """
    Class represents container for dict object with key duplicates.

    Duplicated keys will be stored in different attribute as list, there is no limit for
    number of duplicates.
    """

    def __init__(self, duplicated: list[tuple, tuple]):
        self.data: dict = {}
        self.duplicates: dict = {}
        for d_key, d_val in duplicated:
            if d_key in self.data:
                cur_duplicates = self.duplicates.setdefault(d_key, [])
                cur_duplicates.append(d_val)
                self.duplicates[d_key] = cur_duplicates
            else:
                self.data[d_key] = d_val


def alias_gen_to_camel(source: str) -> str:
    """
    Do alias for pydantic model field.

    Args:
        source (str): source model field name

    Returns:
        source (str): aliased model field name
    """
    return ''.join(word.capitalize() for word in source.split('_'))


class BucketPolicyStatement(BaseModel):
    """
    CLass to represent every BucketPolicy statement.

    There are no required fields.
    """

    # magic attrs to not combine with model fields, by pydantic
    __canonical_user_type__ = 'CanonicalUser'
    __aws_user_type__ = 'AWS'
    __match_everything__ = '*'

    # statement fields
    sid: str = Field(default=None, checking_name='sid')
    effect: Literal['Allow', 'Deny'] = None
    principal: Union[list, str, dict, None] = None
    not_principal: Union[list, str, dict,None] = None
    action: Union[list, str, None] = None
    not_action: Union[list, str, None] = None
    resource: Union[list, str, None] = None
    not_resource: Union[list, str, None] = None
    condition: Union[dict, DictDuplicateKey, None] = None

    # private fields are as containers, they do not participate in field processing
    _condition_duplicated_keys: Optional[dict] = _PROFILE_PREFIX

    class Config:
        allow_population_by_field_name = True
        alias_generator = alias_gen_to_camel
        extra = Extra.forbid
        arbitrary_types_allowed = True

    def get_statement(self) -> dict:
        """
        Create statement dict.

        Exclude None values, logic is based on it.
        Returned field names created by alias generator, logic is based on it.

        Returns:
            statement (dict): created statement
        """
        return self.dict(exclude_none=True, by_alias=True)

    @validator('principal', 'not_principal')
    def _validate_principal(cls, field_value):
        """
        Correct Principal and NotPrincipal field value according Policy structure.

        Args:
            field_value: field value to correct.

        Returns:
            field_value: corrected field value
        """
        if isinstance(field_value, str):

            if field_value == cls.__match_everything__:
                return field_value

            user_type, user_val = cls._determine_user(field_value)

            return {
                user_type: user_val,
            }

        elif isinstance(field_value, list):
            tmp_canonical = {cls.__canonical_user_type__: None}
            tmp_aws = {cls.__aws_user_type__: None}
            principal = {}

            for p_field_value in field_value:

                user_type, user_val = cls._determine_user(p_field_value)

                if user_type == cls.__canonical_user_type__:
                    tmp_canonical = cls._create_principal(user_type, user_val, tmp_canonical)

                elif user_type == cls.__aws_user_type__:
                    tmp_aws = cls._create_principal(user_type, user_val, tmp_aws)

            if tmp_canonical.get(cls.__canonical_user_type__):
                principal.update(tmp_canonical)
            if tmp_aws.get(cls.__aws_user_type__):
                principal.update(tmp_aws)

            return principal

        elif isinstance(field_value, dict):
            return field_value

    @validator('action', 'not_action')
    def _validate_action(cls, field_value):
        """
        Correct Action and NotAction field value according Policy structure.

        Args:
            field_value: field value to correct.

        Returns:
            field_value: corrected field value
        """
        if isinstance(field_value, str):
            return cls._create_action(field_value)

        elif isinstance(field_value, list):
            tmp_actions = []
            for a_field_value in field_value:
                tmp_actions.append(
                    cls._create_action(a_field_value),
                )
            return tmp_actions

    @validator('resource', 'not_resource')
    def _validate_resource(cls, field_value):
        """
        Correct Resource and NotResource field value according Policy structure.

        Args:
            field_value: field value to correct.

        Returns:
            field_value: corrected field value
        """
        if isinstance(field_value, str):
            return cls._create_resource(field_value)

        elif isinstance(field_value, list):
            tmp_resources = []
            for r_field_value in field_value:
                tmp_resources.append(
                    cls._create_resource(r_field_value),
                )
            return tmp_resources

    @validator('condition')
    def _validate_condition(cls, field_value):
        """
        Correct Condition field value according Policy structure.

        Args:
            field_value: field value to correct.

        Returns:
            field_value: corrected field value
        """
        cls._condition_duplicated_keys = None

        if isinstance(field_value, dict):
            return field_value

        elif isinstance(field_value, DictDuplicateKey):
            cls._condition_duplicated_keys = field_value.duplicates
            return field_value.data

    @classmethod
    def _determine_user(cls, source: str) -> (str, str):
        return cls.__canonical_user_type__, source

    @classmethod
    def _create_principal(cls, user_type: str, user_val: str, tmp_principal: dict) -> dict:
        current_canonical = tmp_principal.get(user_type)

        if current_canonical:
            tmp = []
            if isinstance(current_canonical, str):
                tmp = [current_canonical]

            elif isinstance(current_canonical, list):
                tmp = tmp_principal.get(user_type)

            tmp.append(user_val)
            tmp_principal[user_type] = tmp

        else:
            tmp_principal[user_type] = user_val

        return tmp_principal

    @classmethod
    def _create_action(cls, action: str) -> str:
        return 's3:{0}'.format(action)

    @classmethod
    def _create_resource(cls, resource: str) -> str:
        return 'arn:{0}:s3:::{1}'.format(
            cls._partition,
            resource,
        )


class BucketPolicy(object):
    """Class to handle bucket policy."""

    template = {
        'Version': '2012-10-17',
        'Statement': [],
    }

    def __init__(self, statement: list[dict], profile: Optional[str] = None):
        """
        Init class instance.

        Args:
            statement (list[dict]): bucket policy statements according structure
            profile (Optional[str]): s3 profile name
        """
        self.partition = self._determine_partition_by_profile(profile=profile)
        self.statement_structure_class = BucketPolicyStatement

        self.condition_duplicated_keys: dict[str: list] = {}

        self.policy = deepcopy(self.template)
        self.create_policy(statement)

    def create_policy(self, statements: list[dict]):
        """
        Create bucket policy structure.

        Args:
            statements (list[dict]): statements for policy
        """
        for statement in statements:

            self.policy.setdefault('Statement', []).append(
                self.create_statement(**statement),
            )

    def create_statement(self, **kwargs) -> dict:
        """
        Create statement according structure.

        Args:
            kwargs: statement fields, extra fields are prohibited

        Returns:
            statement (dict): created statement
        """
        self.statement_structure_class._partition = self.partition
        statement_structure = self.statement_structure_class(
            **kwargs,
        )

        self.condition_duplicated_keys = statement_structure._condition_duplicated_keys

        return statement_structure.get_statement()

    def as_dict(self, statement_inx: Optional[int] = None) -> dict:
        """
        Return policy as dictionary.

        Args:
            statement_inx (Optional[int]): index of statement to return

        Returns:
            policy (dict): policy (or part) as Python dictionary
        """
        # python considers zero value as false, explicits compare
        if statement_inx or statement_inx == 0:
            if len(self.policy.get('Statement')) > statement_inx:
                return self.policy.get('Statement')[statement_inx]
            else:
                return {'Error': 'Index is out of range.'}

        return self.policy

    def as_string_json(self) -> str:
        """
        Return policy as JSON formatted string.

        Returns:
            policy (str): policy as JSON formatted string
        """
        serialized = json.dumps(
            self.policy,
        )

        if self.condition_duplicated_keys:
            serialized = self._insert_condition_duplicate_keys(serialized)

        return serialized

    def _determine_partition_by_profile(self, profile: Optional[str] = None) -> str:
        """
        Determine partition by prefix of profile name.

        Fields have such structure `arn:partition:service:...`. We need determine `partition`
        (`aws` or `<>`), because we make requests to <> S3 and AWS S3.

        Default value is constants.<>_PROFILE_PREFIX

        Args:
            profile (profile: Optional[str]): profile name

        Returns:
            partition (str) partition for Policy structure
        """
        if profile:
            if profile.startswith(constants.AWS_PROFILE_PREFIX):
                return constants.AWS_PROFILE_PREFIX

        return constants.SERVER_PROFILE_PREFIX

    def _insert_condition_duplicate_keys(self, serialized_policy: str) -> str:
        """
        Insert duplicated condition keys into serialized policy.

        Args:
            serialized_policy (str): serialized policy

        Returns:
            serialized_policy (str): updated serialized policy
        """
        for d_key in self.condition_duplicated_keys.keys():
            for d_val in self.condition_duplicated_keys.get(d_key):

                # cut serialized policy to two pieces in place of duplicated key,
                # and insert such duplicated key(s)
                serialized_policy = serialized_policy.split(d_key, 1)
                serialized_policy.insert(
                    1,
                    '{0}, "{1}'.format(
                        json.dumps({d_key: d_val})[2:-1], d_key,
                    ),
                )

        return ''.join(serialized_policy)


class MountFS(object):
    """Context manager to mount new FS."""

    def __init__(self, target: str):
        """
        Init class instance.

        Args:
            target (str): target to mount new FS
        """
        if os.path.exists(target):
            self.target = target
        else:
            raise FileExistsError

    def __enter__(self):
        with open('tmp.img', 'wb') as file:
            file.truncate(20 * 1024 * 1024)

        cmd.run_blocking(
            command_args='/sbin/mkfs.fat tmp.img',
        )
        cmd.run_blocking(
            command_args=[
                'sudo',
                '-n',
                'mount',
                '-o',
                'loop,uid={0},gid={1}'.format(os.getuid(), os.getgid()),
                'tmp.img',
                '{}'.format(self.target),
            ],
        )

        os.remove('tmp.img')

        return self.target

    def __exit__(self, exc_type, exc_val, exc_tb):
        cmd.run_blocking(
            # lazy unmount to ignore "device is busy" error
            command_args=['sudo', '-n', 'umount', '-l', '{0}'.format(
                self.target,
            )],
        )


class ChangeMode(object):
    """Change mode context manager."""

    def __init__(self, target: str, mode: str):
        """
        Init class instance.

        Args:
            target (str): target to change permissions
            mode (str): permissions octal digit code
        """
        if os.path.exists(target):
            self.target = target
        else:
            raise FileExistsError

        self.mode = mode
        self.current_mode = ''

    def __enter__(self):
        self.current_mode = self.get_current_mode()
        os.chmod(
            path=self.target,
            mode=int(self.mode, 8),
        )
        utils_reaction(
            msg='Permissions for {0} changed from {1} to {2}'.format(
                self.target,
                self.current_mode,
                self.mode,
            ),
            severity=constants.SEV_INFO,
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        if os.path.exists(self.target):
            os.chmod(
                path=self.target,
                mode=int(self.current_mode, 8),
            )
            utils_reaction(
                msg='Permissions for {0} changed back from {1} to {2}'.format(
                    self.target,
                    self.mode,
                    self.get_current_mode(),
                ),
                severity=constants.SEV_INFO,
            )

        else:
            utils_reaction(
                msg='Could not change permission for {0}: path does not exist.'.format(
                    self.target,
                ),
                severity=constants.SEV_WARNING,
            )

    def get_current_mode(self) -> oct:
        """
        Get current permissions for target.

        Returns:
            current_mode (oct): current target permissions
        """
        current_stat = os.stat(
            self.target,
        ).st_mode
        return oct(current_stat)[-3:]  # last 3 digits are file permissions


class ExtendedAttributes(object):
    """Change extended attributes, context manager."""

    def __init__(self, name: str, target: str):
        """
        Init class instance.

        Args:
            name (str): attribute name
            target (str): target to change permissions
        """
        if os.path.exists(target):
            self.target = target
        else:
            raise FileExistsError

        self.name = name
        self.old_value = ''

    @contextmanager
    def set_value(self, attr_value: str):
        """
        Set attribute value.

        Can be used as context manager: return value back.

        Args:
            attr_value (str): value of attribute to set

        Yields:
            None (None): Nothing, `yields` is required by decorator

        """
        self._save_old_value()

        try:
            cmd.run_blocking(
                command_args=[
                    'attr', '-s', self.name, '-V', '"{0}"'.format(attr_value), self.target,
                ],
            )
            yield

        finally:
            cmd.run_blocking(
                command_args=[
                    'attr', '-s', self.name, '-V', '"{0}"'.format(self.old_value), self.target,
                ],
            )

    def get_value(self):
        """
        Return attribute value.

        Returns:
            result (str): result of command.
        """
        return cmd.run_blocking(
            command_args=['attr', '-g', self.name, self.target],
        )

    @contextmanager
    def remove(self):
        """
        Remove attribute.

        Can be used as context manager: return value back.

        Yields:
            None (None): Nothing, `yields` is required by decorator
        """
        self._save_old_value()

        try:
            cmd.run_blocking(
                command_args=['attr', '-r', self.name, self.target],
            )
            yield

        finally:
            cmd.run_blocking(
                command_args=[
                    'attr', '-s', self.name, '-V', '"{0}"'.format(self.old_value), self.target,
                ],
            )

    def list(self):
        """
        Return list of attributes.

        Returns:
            result (str): result of command.
        """
        return cmd.run_blocking(
            command_args=['attr', '-l', self.target],
        )

    def _save_old_value(self):
        """Save old value."""
        try:
            self.old_value = self.get_value()
        finally:
            self.old_value = ''


def check_client_config_file(config_file: str, venv: Optional[str] = None) -> str:
    """
    Check client config file existence, set up venv if it was provided.

    Args:
        config_file (str): client config file
        venv (Optional[str]): venv to set up value equals to config file location

    Returns:
        abs_path (str): absolute path to config file

    Raises:
        FileNotFoundError: if config file does not exists
    """
    abs_path = os.path.abspath(
        os.path.join(  # connect config location with work dir
            constants.WORK_DIR,
            config_file,
        ),
    )

    if os.path.isfile(abs_path):

        if venv:
            os.environ[venv] = abs_path

            utils_reaction(
                msg='Set up venv {0} to {1}'.format(
                    venv, abs_path,
                ),
                severity=constants.SEV_INFO,
            )

        return abs_path

    else:
        raise utils_reaction(
            msg='Config file `{0}` does not exist.'.format(
                abs_path,
            ),
            severity=constants.SEV_CRITICAL,
            returned_exception=FileNotFoundError,
        )


def remove_directory_content(directory: str):
    """
    Remove all content from directory

    Args:
        directory (str): directory to remove content from.
    """
    if os.path.exists(directory):

        try:
            shutil.rmtree(
                directory,
            )
        except Exception as exc:
            utils_reaction(
                msg=[
                    'Remove content from `{0}`: exception.'.format(
                        directory,
                    ),
                    str(exc),
                ],
                severity=constants.SEV_EXCEPTION,
            )
        else:
            utils_reaction(
                msg='Remove content from `{0}`: success.'.format(
                   directory,
                ),
                severity=constants.SEV_INFO,
            )

    else:
        utils_reaction(
            msg='Remove content from `{0}`: does not exist.'.format(
                directory,
            ),
            severity=constants.SEV_WARNING,
        )


def expand_user_path(path: str) -> str:
    """
    Expand user in path if it needed.

    Args:
        path (str): path to process

    Returns:
        path (str): normalized path
    """
    if path.startswith('~'):
        path = os.path.expanduser(path)

    return path


def normalise_win_path(path: str) -> str:
    """
    Normalise Windows path.

    Windows path may contain white spaces.

    Args:
        path (str): path to process

    Returns:
        path (str): normalized path
    """
    path = expand_user_path(path)

    return path.replace(' ', r'\ ')


def write_to_xls(file_name: str, data: list[dict]):
    """
    Create xls doc and write data into it.

    Headers are taken from dict keys. All dicts in data must be same structure.

    Args:
        file_name (str): file name with '.xls' extension to create file
        data (list[dict]): data to write into file
    """
    book = xlwt.Workbook()
    sh = book.add_sheet('New')

    # col index starts from 1 - we have this adjustment in next actions
    sh.col(1).width = 21000
    sh.col(2).width = 7000
    sh.col(3).width = 21000
    sh.col(4).width = 4500

    # headers
    for h_inx, h_data in enumerate(data[0].keys()):
        sh.write(0, h_inx + 1, h_data)

    # data
    for d_inx, ds in enumerate(data):
        for k_inx, key_data in enumerate(data[0].keys()):
            sh.write(d_inx + 2, k_inx + 1, ds.get(key_data, ''))

    if not file_name.endswith('.xls'):
        file_name = '{0}.xls'.format(file_name)

    book.save(file_name)
