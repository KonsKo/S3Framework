"""Constants module."""
import http
import os
import pathlib
import tempfile

from helpers.xml_errors_loader import ErrorsXML

# framework
TMP_DIR = tempfile.mkdtemp()
WORK_DIR = os.path.dirname(__file__)
DEF_REPETITION = 10
DEF_CONFIG_FILE = os.path.join(
    WORK_DIR,
    'conf_framework.json',
)
S3SERVER_PROC_NAME = 'S3SERVER'
S3SERVER_DEFAULT_IP = '127.0.0.1'
S3SERVER_DEFAULT_PORT = '8000'
S3SERVER_DEFAULT_HOST = 'localhost'

# will be set up during framework init if argument and config was provided
SEED_RANDOM_EXTRA = ' '

# according project structure make steps down
S3SERVER_SOURCE_DIR = pathlib.Path(WORK_DIR).parents[1]
S3SERVER_DEFAULT_SRC = os.path.join(
    S3SERVER_SOURCE_DIR,
    'build/S3SERVER',
)

# S3SERVER resets connection in 5 seconds by default,
# that should be a failure condition.
S3SERVER_TIMEOUT = 25
S3SERVER_KILL_TIMEOUT = 5
S3SERVER_HEALTH_URL = 'healthz'
WORDS_FILE_NAME = 'words.txt'

VERIFICATION_FILE_NAME = 'verification.json'
VERIFICATION_FILE_PATH = os.path.join(
    WORK_DIR,
    VERIFICATION_FILE_NAME,
)

IGNOSERVER_TESTS_FILE_NAME = 'ignoSERVER.json'
IGNOSERVER_TESTS_FILE_PATH = os.path.join(
    WORK_DIR,
    IGNOSERVER_TESTS_FILE_NAME,
)

OPENSSL_LIBRARY_PATH = '/opt/lib64/'

# out
PARENT_LOGGER_NAME = 'main'
DEF_S3SERVER_LOG_FILE = os.path.join(
    WORK_DIR,
    'SERVER.log',
)
JOURNAL = os.path.join(
    WORK_DIR,
    'journal.log',
)
LOG_LEVEL = 'DEBUG'
DEF_VERBOSITY = 1
DEF_TEST_PATTERN = 'test_*.py'
DEF_TEST_METHOD_PREFIX = 'test_'

DISCOVERY_GARBAGE = ('.py',)

STRESS_TEST_PATTERN = 'stress_*.py'
STRESS_TEST_METHOD_PREFIX = 'stress_test_'

SEV_INFO = 'info'
SEV_WARNING = 'warning'
SEV_CRITICAL = 'critical'
SEV_ERROR = 'error'
SEV_EXCEPTION = 'exception'
SEV_EXTRA = 'extra'
SEV_SYSTEM = 'system'

SYNC_TIMEOUT = 0.003  # NEVER set up big value


# cmd
SUCCESS_RETURN_CODE = 0


# http
HTTP_STAT = http.HTTPStatus
HTTP_TIMEOUT = 4


# profiles
SERVER_PROFILE_PREFIX = 'SERVER'

SERVER_DEFAULT_PROFILE_NAME = 'SERVER_default'
SERVER_SECOND_PROFILE_NAME = 'SERVER_second_user'
SERVER_ANONYMOUS_PROFILE_NAME = 'SERVER_anonymous'
SERVER_NOT_EXISTENT_PROFILE_NAME = 'SERVER_not_existent_user'


# aws
AWS_PROFILE_PREFIX = 'aws'

AWS_REAL_PROFILE_NAME = 'aws_real'
AWS_SECOND_REAL_PROFILE_NAME = 'aws_real_second'
DEFAULT_REAL_AWS_LOCATION = 'us-west-1'

VENV_AWS_CONFIG_FILE = 'AWS_CONFIG_FILE'

AWS_CLI_TIMEOUT = 10


# commands
EXISTED_COMMANDS_AWS_CLI_S3API = (
    'create_bucket',
    'delete_bucket',
    'head_bucket',
    'get_bucket_acl',
    'put_bucket_acl',
    'get_object',
    'put_object',
    'get_object_acl',
    'put_object_acl',
    'delete_object',
    'create_multipart_upload',
    'upload_part',
    'abort_multipart_upload',
    'complete_multipart_upload',
    'list_multipart_uploads',
    'list_parts',
    'list_buckets',
    'list_objects',
    'list_objects_v2',
    'put_bucket_versioning',
    'get_bucket_versioning',
    'copy_object',
)


EXISTED_COMMANDS_AWS_BOTO = (
    'create_bucket',
    'head_bucket',
    'upload_file',
    'upload_fileobj',
    'head_object',
    'download_fileobj',
    'get_object_acl',
    'get_object_tagging',
    'get_object',
    'put_object_acl',
    'put_object_tagging',
    'put_object',
    'delete_bucket',
    'get_bucket_acl',
    'delete_objects',
    'delete_object',
    'delete_object_tagging',
    'create_multipart_upload',
    'upload_part',
    'abort_multipart_upload',
    'complete_multipart_upload',
    'list_multipart_uploads',
    'list_parts',
    'list_objects',
    'list_buckets',
    'list_objects_v2',
    'put_object_acl',
    'copy_object',
    'upload_part_copy',
    'generate_presigned_post',
    'put_bucket_policy',
    'get_bucket_policy',
    'delete_bucket_policy',
    'get_bucket_encryption',
    'get_object_tagging',
    'put_object_tagging',
    'get_bucket_location',
    'get_caller_identity',
    'put_bucket_versioning',
    'get_bucket_versioning',
    'put_bucket_acl',
    'list_object_versions',  # only for aws for now
    'put_public_access_block',  # only for aws for now
)


ACL_URI_ALL_USERS = 'http://acs.amazonaws.com/groups/global/AllUsers'
ACL_URI_AUTH_USERS = 'http://acs.amazonaws.com/groups/global/AuthenticatedUsers'
ACL_URI_LOG_DELIVERY = 'http://acs.amazonaws.com/groups/s3/LogDelivery'

PERM_READ = 'READ'
PERM_WRITE = 'WRITE'
PERM_READ_ACP = 'READ_ACP'
PERM_WRITE_ACP = 'WRITE_ACP'
PERM_FULL = 'FULL_CONTROL'


# actions commented out are not supported according `policy.h`
BUCKET_POLICY_ALL_ACTIONS = (
    'AbortMultipartUpload',
    # 'BypassGovernanceRetention',
    # 'CreateAccessPoint',
    # 'CreateAccessPointForObjectLambda',
    # 'CreateBucket',
    # 'CreateJob',
    # 'CreateMultiRegionAccessPoint',
    # 'DeleteAccessPoint',
    # 'DeleteAccessPointForObjectLambda',
    # 'DeleteAccessPointPolicy',
    # 'DeleteAccessPointPolicyForObjectLambda',
    'DeleteBucket',
    'DeleteBucketPolicy',
    # 'DeleteBucketWebsite',
    # 'DeleteJobTagging',
    # 'DeleteMultiRegionAccessPoint',
    'DeleteObject',
    'DeleteObjectTagging',
    'DeleteObjectVersion',
    'DeleteObjectVersionTagging',
    # 'DeleteStorageLensConfiguration',
    # 'DeleteStorageLensConfigurationTagging',
    # 'DescribeJob',
    # 'DescribeMultiRegionAccessPointOperation',
    # 'GetAccelerateConfiguration',
    # 'GetAccessPoint',
    # 'GetAccessPointConfigurationForObjectLambda',
    # 'GetAccessPointForObjectLambda',
    # 'GetAccessPointPolicy',
    # 'GetAccessPointPolicyForObjectLambda',
    # 'GetAccessPointPolicyStatus',
    # 'GetAccessPointPolicyStatusForObjectLambda',
    # 'GetAccountPublicAccessBlock',
    # 'GetAnalyticsConfiguration',
    'GetBucketAcl',
    'GetBucketCORS',
    'GetBucketLocation',
    'GetBucketLogging',
    # 'GetBucketNotification',
    # 'GetBucketObjectLockConfiguration',
    # 'GetBucketOwnershipControls',
    'GetBucketPolicy',
    # 'GetBucketPolicyStatus',
    # 'GetBucketPublicAccessBlock',
    # 'GetBucketRequestPayment',
    'GetBucketTagging',
    # 'GetBucketVersioning',
    # 'GetBucketWebsite',
    # 'GetEncryptionConfiguration',
    # 'GetIntelligentTieringConfiguration',
    # 'GetInventoryConfiguration',
    # 'GetJobTagging',
    # 'GetLifecycleConfiguration',
    # 'GetMetricsConfiguration',
    # 'GetMultiRegionAccessPoint',
    # 'GetMultiRegionAccessPointPolicy',
    # 'GetMultiRegionAccessPointPolicyStatus',
    'GetObject',
    'GetObjectAcl',
    # 'GetObjectAttributes',
    # 'GetObjectLegalHold',
    # 'GetObjectRetention',
    'GetObjectTagging',
    # 'GetObjectTorrent',
    'GetObjectVersion',
    'GetObjectVersionAcl',
    # 'GetObjectVersionAttributes',
    # 'GetObjectVersionForReplication',
    'GetObjectVersionTagging',
    # 'GetObjectVersionTorrent',
    # 'GetReplicationConfiguration',
    # 'GetStorageLensConfiguration',
    # 'GetStorageLensConfigurationTagging',
    # 'GetStorageLensDashboard',
    # 'InitiateReplication',
    # 'ListAccessPoints',
    # 'ListAccessPointsForObjectLambda',
    # 'ListAllMyBuckets',
    'ListBucket',
    'ListBucketMultipartUploads',
    'ListBucketVersions',
    # 'ListJobs',
    # 'ListMultiRegionAccessPoints',
    'ListMultipartUploadParts',
    # 'ListStorageLensConfigurations',
    # 'ObjectOwnerOverrideToBucketOwner',
    # 'PutAccelerateConfiguration',
    # 'PutAccessPointConfigurationForObjectLambda',
    # 'PutAccessPointPolicy',
    # 'PutAccessPointPolicyForObjectLambda',
    # 'PutAccessPointPublicAccessBlock',
    # 'PutAccountPublicAccessBlock',
    # 'PutAnalyticsConfiguration',
    'PutBucketAcl',
    'PutBucketCORS',
    'PutBucketLogging',
    # 'PutBucketNotification',
    # 'PutBucketObjectLockConfiguration',
    # 'PutBucketOwnershipControls',
    'PutBucketPolicy',
    # 'PutBucketPublicAccessBlock',
    # 'PutBucketRequestPayment',
    'PutBucketTagging',
    'PutBucketVersioning',
    # 'PutBucketWebsite',
    # 'PutEncryptionConfiguration',
    # 'PutIntelligentTieringConfiguration',
    # 'PutInventoryConfiguration',
    # 'PutJobTagging',
    # 'PutLifecycleConfiguration',
    # 'PutMetricsConfiguration',
    # 'PutMultiRegionAccessPointPolicy',
    'PutObject',
    'PutObjectAcl',
    # 'PutObjectLegalHold',
    # 'PutObjectRetention',
    'PutObjectTagging',
    'PutObjectVersionAcl',
    'PutObjectVersionTagging',
    # 'PutReplicationConfiguration',
    # 'PutStorageLensConfiguration',
    # 'PutStorageLensConfigurationTagging',
    # 'ReplicateDelete',
    # 'ReplicateObject',
    # 'ReplicateTags',
    # 'RestoreObject',
    # 'UpdateJobPriority',
    # 'UpdateJobStatus',
)

# will be filled during framework init, for codes description take a look to `error_xml.h`
XML_ERRORS_SOURCE_FILE_NAME = 'error_xml.h'
XML_ERRORS: ErrorsXML = ErrorsXML()


# docker-compose
COMPOSE_FILE_NAME = 'docker-compose.yml'
COMPOSE_FILE = os.path.join(
    S3SERVER_SOURCE_DIR,
    COMPOSE_FILE_NAME,
)
SERVER_CLI_DEFAULT_SERVICE_NAME = 'SERVERclient'


# hadoop
HADOOP_HOME = '/usr/local/hadoop'


# s3browser
S3B_ACC_VFS = 'default_vfs'
S3B_ACC_SERVERFS = 'default_SERVERfs'
S3B_LOG_LOCATION = '~/.wine/drive_c/users/cat2/Application Data/S3Browser/logs'


# limits
OBJECT_KEY_MAX_LENGTH = 1024
OBJECT_KEY_MAX_COMPONENT_LENGTH = 234
TAG_KEY_MAX_LENGTH = 128
TAG_VAL_MAX_LENGTH = 256
TAG_MAX_NUMBER = 10
LIM_ATTR_ACL_VALUE_SIZE_VFS = 3766  # got empirically
LIM_ATTR_ACL_VALUE_SIZE_SERVERFS = 65298  # got empirically
