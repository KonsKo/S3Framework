#!/usr/bin/env python3

"""Script to remove all buckets and objects from S3 account."""
import argparse
import asyncio
import sys

import constants
from helpers.s3_tools import AccountCleaner
from helpers.utils import check_client_config_file, output_handler

reaction = output_handler.OutputReaction(
    module_name=__name__,
    prefix='script',
)


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
        '-C',
        '--config',
        help='Path to aws/s3 config file.',
        type=str,
        dest='config_file',
    )
    parser.add_argument(
        '-P',
        '--profile',
        help='Profile name from `config` to do actions from.',
        type=str,
        dest='profile',
    )
    parser.add_argument(
        '-F',
        '--filter-buckets',
        help='List if bucket names to filter deletion.',
        type=str,
        dest='filter_buckets',
        nargs='+',
    )

    return parser.parse_args()


async def main():

    command_args = set_up_arg_parser()
    config_file = command_args.config_file
    profile = command_args.profile
    filter_buckets = command_args.filter_buckets

    if not all({config_file, profile}):
        reaction(
            msg='Arguments are required: `--config`, `--profile`. Use `--help` for details',
            severity=constants.SEV_ERROR,
        )
        sys.exit(1)

    check_client_config_file(config_file, venv=constants.VENV_AWS_CONFIG_FILE)

    cleaner = AccountCleaner(profile_name=profile)
    await cleaner.clean(filter_buckets=filter_buckets)


if __name__ == '__main__':
    asyncio.run(main())
