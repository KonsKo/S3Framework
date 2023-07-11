"""Module to parse error xml file and load errors."""
import re

from pydantic import BaseModel, Extra


class ErrorXML(BaseModel):
    """XML error structure."""

    error_code: str
    error_text: str
    response_code: int


class ErrorsXML(BaseModel, extra=Extra.allow):
    """
    Structure for all XML errors.

    Whole structure is creating dynamically from existed file: `error_xml.h`.
    """


def load_xml_errors(source: str) -> dict:
    """
    Load predefined error codes from `source`.

    Code to parse looks like:
        ...
        XX(OK, "No error.", 200) \
        XX(AccessControlListNotSupported, "The bucket does not allow ACLs.", 400) \
        XX(AccessDenied, "Access Denied", 403) \
        ...

    Args:
        source (str): source file to get xml errors

    Returns:
        all_errors (dict): all loaded errors

    """
    all_xml_errors: dict[str, ErrorXML] = {}

    with open(source, 'r') as xml_errors_file:
        xml_errors_data = xml_errors_file.readlines()

    for line in xml_errors_data:

        # take a line such `  XX(...`
        if re.search(pattern=r'^\s*XX', string=line):

            # take part `OK, "No error.", 200` from line `XX(OK, "No error.", 200) \`
            error_line = re.search(
                pattern=r'XX\((.+)\)\s\\',
                string=line.strip(),
            ).group(1)

            # split by '"' (double quote), to get ('OK, ', 'No error.', ', 200')
            error_line = tuple(error_line.split('"'))

            e_xml = ErrorXML(
                error_code=error_line[0].strip().replace(',', ''),
                error_text=error_line[1].strip(),
                response_code=int(
                    re.search(string=error_line[2], pattern=r'\d+').group(),
                ),
            )
            all_xml_errors[e_xml.error_code] = e_xml

    return all_xml_errors
