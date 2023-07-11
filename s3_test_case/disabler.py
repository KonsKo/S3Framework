"""Module for test ignoring."""
import json
from typing import Optional

import constants
from helpers import output_handler

dis_reaction = output_handler.OutputReaction(
    prefix='test',
    module_name=__name__,
)


class BrokenTests(object):
    """Ignored tests representation."""

    def __init__(self, tests: list[dict], search_field: str = 'name'):
        """
        Init class instance.

        Args:
            tests (list[dict]): ignored tests
            search_field (str): field to process `self.get` method
        """
        self.search_field = search_field
        self._tests = tests

    @property
    def tests(self) -> list[dict]:
        """
        Getter for `self._tests`.

        Returns:
            self._tests (list[dict]): list of ignored tests
        """
        return self._tests

    @tests.setter
    def tests(self, tests_value: list[dict]):
        """
        Setter for `self._tests`.

        Args:
            tests_value (list[dict]): tests value to set up

        Raises:
            TypeError: if type error
            KeyError: if key error
        """
        if not isinstance(tests_value, list):
            raise TypeError('`tests` must be in `list` type.')

        for test_v in tests_value:
            if not isinstance(test_v, dict):
                raise TypeError('Each test must be in `dict` type.')

            if self.search_field not in test_v.keys():
                raise KeyError('Each test must have `name` field.')

        self._tests = tests_value

    def update(self, test_id: str):
        """
        Update test list.

        Args:
            test_id (str): test ID
        """
        if test_id not in self.get_tests_ids():

            try:
                self.tests.append(
                    {'name': test_id, 'reason': ''},
                )
            except Exception as exc:
                dis_reaction(
                    msg=['Failed to update tests.', exc],
                    severity=constants.SEV_ERROR,
                )

    def get(self, by_key: str) -> Optional[dict]:
        """
        Check for substring equality with test[self.search_field].

        Args:
            by_key (str): substring

        Returns:
            Optional[dict]: ignored test data if found
        """
        if self.tests:
            for test in self.tests:
                if test.get(self.search_field) == by_key:
                    return test

        return None

    def get_tests_ids(self) -> tuple:
        """
        Get all ignored tests IDs as tuple.

        Returns:
            ids (tuple): ignored tests ids
        """
        if self.tests:
            return tuple(test.get('name') for test in self.tests)


def load_broken_tests(source: str) -> BrokenTests:
    """
    Load ignored tests data.

    Args:
        source (str): source file with tests to load

    Returns:
        ignored_test_data (list[dict]): copy of ignored test data
    """
    try:
        with open(source, 'r') as dtf:
            return BrokenTests(
                json.load(dtf),
            )
    except Exception as exc:
        dis_reaction(
            msg=['Failed to load broken tests.', exc],
            severity=constants.SEV_WARNING,
        )
