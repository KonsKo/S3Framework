"""
Module to parse function/method docstring and get requested info.

Example of docstring:

    '''
    Check bucket policy processing with explicit absence of fields.

    Test subject:
        self

    Test description:
        Check bucket policy processing with explicit absence of fields.

    Checked with AWS:
        False

    '''

"""
import copy
from typing import Callable, Union

import constants
from helpers.output_handler import OutputReaction


class DocstringParser(object):
    """Class to parse function/method docstring."""

    reaction = OutputReaction(
        module_name=__name__,
        prefix='REPORT',
    )

    report_element = {
        'Test subject:': '',
        'Test description:': '',
        'Checked with AWS:': '',
    }

    def __init__(self):
        self.current_docstring = ''
        self.current_element = {}

    def parse(self, source: Union[Callable, str, list]) -> dict:
        """
        Parse function/method docstring.

        Args:
             source (Callable): source to get docstring info

        Returns:
            (dict): parsed docstring elements
        """
        if not source:
            return {}

        self.current_element = copy.deepcopy(self.report_element)

        if callable(source):
            self.current_docstring = self.get_docstring(source)
        elif isinstance(source, str):
            self.current_docstring = source.splitlines()
        else:
            self.current_docstring = source

        self.parse_args()

        docstring_elements = self.current_element

        self.current_element = {}
        self.current_docstring = ''

        return docstring_elements

    def get_docstring(self, func: Callable) -> list[str]:
        """
        Get docstring data from function.

        Args:
            func (Callable): func name to get docstring info

        Returns:
            list[str]: function docstring, split by new lines
        """
        if callable(func):

            if func.__doc__:
                self.current_docstring = func.__doc__.splitlines()

            else:
                self.reaction(
                    msg='Docstring for `{0}` is empty.'.format(func),
                    severity=constants.SEV_WARNING,
                )

            return self.current_docstring

        self.reaction(
            msg='Provided argument `{0}` is not callable.',
            severity=constants.SEV_WARNING,
        )
        return []

    def parse_args(self):
        """Get docstring info and parse requested info."""
        for field in self.report_element.keys():
            for il, line in enumerate(self.current_docstring):
                line = line.strip()
                if line.startswith(field):
                    self._parse_arg(il, field)
                    break

            else:
                break

                # TODO raise after all tests are prepared
                #raise self.reaction(
                #    msg='Docstring argument `{0}` was not found.'.format(field),
                #    severity=constants.SEV_EXCEPTION,
                #    returned_exception=AttributeError,
                #)

    def _parse_arg(self, start_inx: int, element_name: str):
        """
        Parse argument from docstring.

        Argument start with argument name, i.e. `Args:` or `Test subject:`.
        Argument value may start on same line or on next new line.
        Argument finishes with empty line or end-of-docstring.

        Args:
            start_inx (int): index where arg is starts
            element_name (str): name of arg
        """
        # arg data may start on same line as arg name
        parsed_data = self.current_docstring[start_inx].replace(element_name, '')
        start_inx += 1
        while start_inx < len(self.current_docstring):
            if self.current_docstring[start_inx]:
                parsed_data += ' {0}'.format(
                    self.current_docstring[start_inx].strip(),
                )

            # parse until new (empty) line
            else:
                break

            start_inx += 1

        self.current_element[element_name] = parsed_data.strip()


docstring_parser = DocstringParser()
