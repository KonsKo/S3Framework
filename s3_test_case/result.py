"""Test result module."""
import time
import unittest

import constants
from helpers import framework, output_handler

result_reaction = output_handler.OutputReaction(
    prefix='result',
    module_name=__name__,
)


# it is almost exact copy of unittest.TextTestResult.startTest
# except trailing new line character
class S3TextTestResult(unittest.TextTestResult):
    """Result class for tests."""

    long_line = '\n----------------------------------------------------------------------\n\n'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_start = 0
        self.all_results = {}
        self.all_ignored = {}
        self.all_skipped = {}

    def addError(self, test, err):  # noqa:N802
        super().addError(test, err)

        # if error, set up flag
        framework.structure.test_had_error = True

    def startTest(self, test):  # noqa:N802
        """
        Start test.

        Args:
            test: test to start
        """
        self.stream.write(self.long_line)
        self.test_start = time.perf_counter()

        # skip direct parent call to avoid double printing of test name
        unittest.result.TestResult.startTest(self, test)

        # from TextTestResult.startTest()
        if self.showAll:
            self.stream.write(self.getDescription(test))
            self.stream.write(' ... \n')   # <--
            self.stream.flush()

    # overwrite method to catch and combine all auto skipped tests
    def addSkip(self, test, reason):
        super().addSkip(test, reason)

        time_taken = time.perf_counter() - self.test_start
        test.description = reason

        self.all_skipped[test] = time_taken

    def stopTest(self, test):  # noqa:N802
        super().stopTest(test)

        time_taken = time.perf_counter() - self.test_start

        if test.is_ignored:
            self.all_ignored[test] = time_taken
        else:
            self.all_results[test] = time_taken

        result_reaction(
            msg='Test running has taken {0} seconds\n'.format(
                str(round(time_taken, 3)),
            ),
            severity=constants.SEV_INFO,
        )

    def stopTestRun(self):  # noqa:N802
        super().stopTestRun()

        to_show = 20
        longest_tests = dict(
            sorted(
                self.all_results.items(), key=lambda result: result[1], reverse=True,
            )[:to_show],
        )

        if longest_tests:
            msg = 'First {0} longest tests:'.format(
                str(to_show) if to_show <= len(longest_tests) else str(len(longest_tests)),
            )
            self.print_out_test_results(longest_tests, header_msg=msg)

        if self.all_ignored:
            self.print_out_test_results(
                self.all_ignored, header_msg='All ignored tests:',
            )

        if self.all_skipped:
            self.print_out_test_results(
                self.all_skipped, header_msg='All auto skipped tests:',
            )

        self._make_report_ignored_tests()

    def print_out_test_results(self, test_results: dict, header_msg: str = ''):
        result_reaction(
            msg=header_msg,
            severity=constants.SEV_INFO,
        )
        self.stream.write(self.long_line)
        self.stream.write(
            '    {0:<120}   {1:<20}   {2:<20}\n\n'.format(
                'Test ID:',
                'Time taken:',
                'Info:',
            ),
        )
        for test_key, test_val in test_results.items():
            self.stream.write(
                '    {0:<120}   {1:<20}   {2:<20}\n'.format(
                    test_key.id(),
                    str(round(test_val, 3)),  # rounded test time
                    test_key.description,
                ),
            )
        self.stream.write('\n')

    def _make_report_ignored_tests(self):
        """Make report for all ignored tests for all time."""
        ignored_tests = framework.structure.db.select_tests()

        result_reaction(
            msg='Report for all ignored tests:',
            severity=constants.SEV_INFO,
        )
        self.stream.write(self.long_line)

        self.stream.write(
            '    {0:<120}   {1:<20}   {2:<20}\n\n'.format(
                'Test ID:',
                'Launches - Fails:',
                'RED FS Launches - Fails:',
            ),
        )

        for test in ignored_tests:
            self.stream.write(
                '    {0:<120}   {1:<20}   {2:<20}\n'.format(
                    test.test_id,
                    '{0:>8} - {1}'.format(test.num_of_runs, test.num_of_fails),
                    '{0:>15} - {1}'.format(test.num_of_runs_redfs, test.num_of_fails_redfs),
                ),
            )
