#!/usr/bin/env python3
"""
S3 test framework.

It is possible to run script with no arguments. In this case application is
going to try to discover all tests from current working directory.
Boto3 as AWS CLI uses config file: `/home/<username>/.aws/credentials`.
Take a look at README.md.
"""

import argparse
import json
import logging
import os
import sys

import constants
from helpers import exceptions, framework, output_handler
from s3_test_case.loader import S3TestLoader

# init main logger
logger = logging.getLogger(
    constants.PARENT_LOGGER_NAME,
)
logger.setLevel('DEBUG')


def set_logger(
    file_path: str = constants.JOURNAL,
    log_level: str = constants.LOG_LEVEL,
) -> None:
    """
    Set up logger as journal for events.

    Args:
        log_level (str): Logger level.
        file_path (str): File path to save log.

    Raises:
        Exception: if error in logger setting up.
    """
    try:
        logger_handler = logging.FileHandler(
            file_path,
            mode='w',
        )
    except Exception:
        logger.exception(
            'Logger has error while setting up handler',
        )
        raise

    try:
        logger.setLevel(log_level.upper())
    except Exception:
        logger.exception(
            'Logger has error while setting up level.',
        )

    formatter = logging.Formatter(
        '{asctime} :: {name:22s} :: {levelname:8s} :: {message}',
        style='{',
    )

    logger_handler.setFormatter(formatter)
    logger.addHandler(logger_handler)
    logger.info(
        'Logger has been successfully set up.',
    )


def load_config(config_file: str) -> dict:
    """
    Load configuration from json-file and structuring into a class-notation.

    Args:
        config_file (str): path to config file

    Raises:
        Exception: if error while getting configuration.

    Returns:
        loaded_config (dict): Config for Zabbix, Redis, Psql, logger.
    """
    with open(config_file, 'r') as cf:
        try:
            loaded_config = json.loads(cf.read())
        except Exception:
            logger.exception(
                'Error getting configuration',
            )
            raise
    return loaded_config


def set_up_arg_parser() -> argparse.Namespace:
    """
    Set up and process argument parser.

    Returns:
        parsed_arguments (argparse.Namespace): parsed arguments
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
    )
    parser.add_argument(
        'tests',
        help="""
        Test-related name(s).
        It may be:
        - full dotted test name(s)
        - part of dotted test name, such only test class name or test method name
        - any substring
        """,
        default=[],
        nargs='*',
        type=str,
    )
    parser.add_argument(
        '--port',
        help='Port number to run S3 server.',
        type=int,
    )
    parser.add_argument(
        '--no-display-info',
        help="""
        If set, do not display info to console output.
        Duplicates config option `display_info`, but has more priority.
        """,
        dest='display_info',
        action='store_false',
    )
    parser.add_argument(
        '-C',
        '--config',
        help="""
        Path to `json` file with configurations.
        If not provided: config  = dir(__file__) / DEF_CONFIG_FILE_NAME
        """,
        type=str,
        default=constants.DEF_CONFIG_FILE,
        dest='config_file',
    )
    parser.add_argument(
        '--aws-config',
        help="""
            Path to aws config file.
            Default location is `~/.aws/config`
            """,
        type=str,
        dest='aws_config_file',
    )
    parser.add_argument(
        '--s3cmd-config',
        help="""
            Path to s3cmd config file.
            Default location is `~/.s3cfg`
            """,
        type=str,
        dest='s3cmd_config_file',
    )
    parser.add_argument(
        '--stress',
        help="""
            If set, framework will test stress scenarios only.
            Test method prefix changed to '{0}'.
            Test discovery pattern changed to `{1}`.
            """.format(
            constants.STRESS_TEST_METHOD_PREFIX, constants.STRESS_TEST_PATTERN,
        ),
        dest='is_stress',
        action='store_true',
    )
    parser.add_argument(
        '--to-aws',
        help='If set, framework will send requests to real AWS S3 server.',
        dest='to_aws',
        action='store_true',
    )
    parser.add_argument(
        '--to-fs',
        help='If set, framework will work with <> FS.',
        dest='to_fs',
        action='store_true',
    )
    parser.add_argument(
        '-CF',
        '--compose-file',
        help='Docker-compose file.',
        dest='compose_file',
        default=constants.COMPOSE_FILE,
    )
    parser.add_argument(
        '-CS',
        '--compose-service',
        help='Docker-compose service name.',
        dest='compose_service',
        default=constants.SERVER_CLI_DEFAULT_SERVICE_NAME,
    )
    parser.add_argument(
        '-SR',
        '--seed-random',
        help='Extra parameter to generate random seed.',
        dest='seed_random',
        default=None,
    )
    parser.add_argument(
        '--drop-cap',
        help="""
            Remove capabilities [cap_dac_override, cap_dac_read_search] 
            from the prevailing bounding set for server.
        """,
        dest='drop_cap',
        action='store_true',
    )
    parser.add_argument(
        '--clean',
        help="""
            Remove all content that framework was created during the work.
            For some limitations, possible to clean server s3 root running on VFS only.

            Parameter can be set up via config file.
        """,
        dest='clean',
        action='store_true',
    )

    return parser.parse_args()


if __name__ == '__main__':
    main_reaction = output_handler.OutputReaction(
        module_name=__name__,
        prefix='main',
    )

    command_args = set_up_arg_parser()

    framework_conf = load_config(config_file=command_args.config_file)

    if not command_args.display_info:
        framework_conf['display_info'] = False

    if command_args.port:
        framework_conf['servers3']['listen_port'] = command_args.port

    if command_args.seed_random:
        constants.SEED_RANDOM_EXTRA = command_args.seed_random
        main_reaction(
            msg='Seed random extra parameter was set up: `{0}`'.format(command_args.seed_random),
            severity=constants.SEV_INFO,
        )

    # The most correct way would be to implement a folder/prefix config option
    if not framework_conf['servers3']['log']:
        if command_args.to_fs:
            # Build number can be deduced from port number. The port number is
            # the main source of potential conflicts between instances anyway.
            framework_conf['servers3']['log'] = os.path.join(
                constants.WORK_DIR,
                'servers3-{0}.log'.format(framework_conf['servers3']['listen_port']),
            )
        else:
            framework_conf['servers3']['log'] = constants.DEF_S3SERVER_LOG_FILE

    if command_args.aws_config_file:
        framework_conf['aws_config'] = command_args.aws_config_file

    if command_args.s3cmd_config_file:
        framework_conf['s3cmd_config'] = command_args.s3cmd_config_file

    try:
        framework_conf['servers3']['compose_file'] = command_args.compose_file
        framework_conf['servers3']['compose_service'] = command_args.compose_service
    except KeyError as k_err:
        main_reaction(
            msg=['Failed to set up server `compose`-related config', k_err],
            severity=constants.SEV_WARNING,
        )

    set_logger()

    framework.start_entity(
        config=framework_conf,
        to_aws=command_args.to_aws,
        to_fs=command_args.to_fs,
        drop_cap=command_args.drop_cap,
        do_cleanup=command_args.clean if command_args.clean else framework_conf.get(
            'do_clean_up', False,
        ),
    )

    if command_args.is_stress:
        pattern = constants.STRESS_TEST_PATTERN
        method_prefix = constants.STRESS_TEST_METHOD_PREFIX
    else:
        pattern = constants.DEF_TEST_PATTERN
        method_prefix = constants.DEF_TEST_METHOD_PREFIX

    test_loader = S3TestLoader(
        test_conf=framework_conf['tests'],
        pattern=pattern,
        method_prefix=method_prefix,
    )

    try:
        test_results = test_loader.discover_and_run_tests(
            test_names=command_args.tests,
        )

    except exceptions.TestDiscoveryFailedError:
        test_results = None

    except Exception:
        raise main_reaction(
            msg='Failed to run tests',
            severity=constants.SEV_CRITICAL,
            returned_exception=exceptions.FrameworkRunTimeError,
        )

    finally:
        framework.structure.tear_down()
        main_reaction(
            msg='Framework has been stopped.',
            severity=constants.SEV_INFO,
        )

    # if results of test has any issue change return code
    if test_results and (test_results.errors or test_results.failures):
        sys.exit(1)

    sys.exit(0)
