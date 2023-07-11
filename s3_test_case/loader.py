"""Test loader module."""
import sys
import unittest
from typing import Optional, Union

import constants
from helpers import exceptions, output_handler
from s3_test_case.result import S3TextTestResult

loader_reaction = output_handler.OutputReaction(
    prefix='loader',
    module_name=__name__,
)

DISCOVERY_EXCEPTIONS = (AttributeError, ImportError, ModuleNotFoundError)
ERROR_DISCOVERY_ZERO = """

    *** No tests were found for argument: < {argument} >. Process will be stopped. ***

"""


def clean_test_name(name: str) -> str:
    """
    Clean test name from garbage symbols.

    Some symbols, such `.py` produce inconvenience for creating dotted test name.

    Args:
        name (str): test name to clean

    Returns:
        name (str): cleaned test name
    """
    for prefix in constants.DISCOVERY_GARBAGE:
        if prefix in name:
            loader_reaction(
                msg='Removed `{0}` from name `{1}`'.format(prefix, name),
                severity=constants.SEV_EXTRA,
            )
            name = name.replace(prefix, '')

    return name


# noinspection PyTypeChecker
class S3TestLoader(unittest.TestLoader):
    """Test loader class implementation."""

    def __init__(self, test_conf: dict, pattern: str, method_prefix: str):
        """
        Init class instance.

        Args:
            test_conf (dict): test-related config
            pattern (str): pattern for discovery, i.e. prefix for test directory
            method_prefix (str): test method prefix to discovery

        """
        super().__init__()
        self.conf = test_conf
        self.test_quantity = 0
        self.all_discovered_tests = self.suiteClass([])
        self.pattern = pattern
        self.testMethodPrefix = method_prefix

    def discover_and_run_tests(
        self,
        test_names: list[str],
    ) -> unittest.TestResult:
        """
        Discover and run tests cases, returning the result.

        Args:
            test_names (list[str]): list of test names

        Returns:
            test_results (unittest.TestResult)
        """
        # discover all tests from WORK_DIR one time and filter result if it needed
        self.discover_all_tests()

        if test_names:
            filtered_tests = []

            # walk over all names, filter them and append to one list, remove duplicates
            for test_name in test_names:

                test_name = clean_test_name(test_name)

                filtered_tests.extend(
                    self.filter_tests_by_name(test_name),
                )

            filtered_suite = self.suiteClass(
                list(set(filtered_tests)),
            )

        # empty test_names - no filtering - take all tests
        else:
            filtered_suite = self.all_discovered_tests

        self._count_discovered_tests(filtered_suite)

        if self.test_quantity > 0:
            loader_reaction(
                msg='Discovered < {0} > test(s) and they are ready to invoke.'.format(
                    str(self.test_quantity),
                ),
                severity=constants.SEV_INFO,
            )
        else:
            raise loader_reaction(
                msg=ERROR_DISCOVERY_ZERO.format(argument=test_names),
                severity=constants.SEV_CRITICAL,
                returned_exception=exceptions.TestDiscoveryFailedError,
            )

        if self.conf.get('results'):
            return self._run_with_file_stream(
                discovered_tests=filtered_suite,
            )

        return self._run_with_stdout_stream(
            discovered_tests=filtered_suite,
        )

    def filter_tests_by_name(self, name: str) -> list:
        """
        Filter out tests by name.

        !!! Filter by exact match of part(s). !!!

        Args:
            name (str): string to filter tests

        Returns:
            tests (list): filtered tests
        """
        filtered_tests = []

        for test in self.all_discovered_tests._tests:

            # `dot` is divider in test name-path,
            # means complicated name such `<test_module>.<test_case>`
            # maximum 3 dots
            num_of_dots = name.count('.')

            # `3 dots` is full test name `<test_directory>.<test_module>.<test_case>.<test>`
            # match for exact test path-name
            if num_of_dots == 3 and name == test.id():
                filtered_tests.append(test)

            # one part of test name-path, such only test case name or only test name
            elif num_of_dots == 0 and name in test.id().split('.'):
                filtered_tests.append(test)

            # two or three parts of test name-path
            # part may be in the start or end on test id, or also in the middle (for 2 dots)
            elif num_of_dots in {1, 2}:

                test_id_parts = test.id().split('.')

                # we need to check every test id contains provided name in related place
                for part_range in range(4 - num_of_dots):
                    if name == '.'.join(test_id_parts[part_range:part_range + num_of_dots + 1]):
                        filtered_tests.append(test)
                        break

        return filtered_tests

    def discover_all_tests(self):
        """Discover all tests from WORK_DIR."""
        self.merge_discovered_tests(
            self.discover_from_dir(),
        )

    def merge_discovered_tests(
        self, discovered_tests: Union[unittest.TestCase, unittest.TestSuite],
    ) -> None:
        """
        Merge discovered tests to one test suite.

        Args:
            discovered_tests (Union[unittest.TestCase, unittest.TestSuite]): tests to merge
        """
        if isinstance(discovered_tests, self.suiteClass):
            for dt in discovered_tests:
                if isinstance(dt, self.suiteClass):
                    self.merge_discovered_tests(dt)
                elif isinstance(dt, unittest.loader._FailedTest):

                    # to catch errors, otherwise, exceptions inside test are hidden
                    raise dt._exception

                else:
                    self.all_discovered_tests.addTest(dt)

    def discover_from_dir(self, start_dir: Optional[str] = None) -> unittest.TestSuite:
        """
        Discover tests from start directory.

        Args:
            start_dir (Optional[str]): directory to discover tests from

        Returns:
            tests (unittest.TestSuite): discovered tests
        """
        if not start_dir:
            start_dir = constants.WORK_DIR

        try:
            return self.discover(
                start_dir=start_dir,
                pattern=self.pattern,
                top_level_dir=constants.WORK_DIR,
            )

        except DISCOVERY_EXCEPTIONS as dis_exception:
            raise loader_reaction(
                msg=[
                    'Wrong start directory name for test discovery.',
                    dis_exception,
                ],
                severity=constants.SEV_CRITICAL,
                returned_exception=exceptions.TestDiscoveryFailedError,
            )

    def _count_discovered_tests(self, tests):
        for d_test in tests._tests:
            if isinstance(d_test, unittest.TestCase):
                self.test_quantity += 1
            if isinstance(d_test, unittest.TestSuite):
                self._count_discovered_tests(d_test)

    def _run_with_stdout_stream(
        self,
        discovered_tests: Union[unittest.TestSuite, unittest.TestCase],
    ) -> unittest.TestResult:
        """
        Run tests and put result to file.

        Args:
            discovered_tests (Union[unittest.TestSuite, unittest.TestCase]):
                discovered test to run

        Returns:
            results (unittest.TestResult): test results
        """
        loader_reaction(
            msg='Test results are gonna be shown in console',
            severity=constants.SEV_INFO,
        )
        runner = unittest.TextTestRunner(
            stream=sys.stdout,
            verbosity=self.conf.get('verbosity', constants.DEF_VERBOSITY),
            resultclass=S3TextTestResult,
        )
        return runner.run(discovered_tests)

    def _run_with_file_stream(
        self,
        discovered_tests: Union[unittest.TestSuite, unittest.TestCase],
    ) -> unittest.TestResult:
        """
        Run tests and put result to stdout.

        Args:
            discovered_tests (Union[unittest.TestSuite, unittest.TestCase]):
                discovered test to run

        Returns:
            results (unittest.TestResult): test results
        """
        with open(self.conf.get('results'), 'w') as out_file:
            loader_reaction(
                msg='Test results are in file: `{0}`.'.format(
                    self.conf.get('results'),
                ),
                severity=constants.SEV_INFO,
            )
            runner = unittest.TextTestRunner(
                stream=out_file,
                verbosity=self.conf.get('verbosity', constants.DEF_VERBOSITY),
            )
            return runner.run(discovered_tests)
