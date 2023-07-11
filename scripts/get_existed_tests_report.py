"""Script to get tests information with different parameters."""
import argparse

import constants
from helpers import framework, utils
from helpers.docstring_parser import docstring_parser
from s3test import load_config
from s3_test_case.loader import S3TestLoader


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
        '--out',
        help='File/path, in which case the report file is saved.',
        type=str,
        dest='out_file',
        default='report.xls',
    )

    return parser.parse_args()


if __name__ == '__main__':

    command_args = set_up_arg_parser()

    # unittest during test discovery calls test instances.
    # our test cases depends on framework instance,
    # hence, we start FW to just discover tests
    framework.start_entity(
        config=load_config(constants.DEF_CONFIG_FILE),
    )

    test_loader = S3TestLoader(
        test_conf={},
        pattern=constants.DEF_TEST_PATTERN,
        method_prefix=constants.DEF_TEST_METHOD_PREFIX,
    )
    test_loader.discover_all_tests()

    docstrings = []
    for test in test_loader.all_discovered_tests:

        test_structure = {'Test name:': test.id()}

        docstring = docstring_parser.parse(test._testMethodDoc)
        test_structure.update(docstring)

        docstrings.append(test_structure)

    utils.write_to_xls(file_name=command_args.out_file, data=docstrings)
