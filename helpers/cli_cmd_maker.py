"""Module to command line maker: create cmd for different clients."""
from typing import Any, Optional

from pydantic import BaseModel, Extra, Field

import constants
from helpers import output_handler

cmd_maker = output_handler.OutputReaction(
    prefix='CMD MAKER',
    module_name=__name__,
)


class CommandMapperBase(BaseModel):
    """
    Base class to create command line for different clients.

    Logic behind the fields mapping:
        - field option `glob: boolean` - field is applied to `main_command` or `command_group`
        - field option `option: str` - in-client option name
        - field type Bool - `option` is applied as flag without value

    Order of fields inside class - order of arguments in created command.
    """

    main_command: str
    command_group: Optional[str] = Field(
        description='Command group, such `s3` or `s3api` for `awscli`',
        default='',
    )
    inner_command: Optional[str] = Field(
        description='Inner command name, which usually depends on `self.command_group`',
        default='',
    )

    # create cmd by parts to keep correct order of attributes
    global_attrs = ''
    inner_command_attrs = ''

    # divider between attribute name and value
    attr_divider = ' '

    # fields names, will not be included in processing
    _excluded_fields = {
        'main_command',
        'command_group',
        'inner_command',
        'global_attrs',
        'inner_command_attrs',
        'attr_divider',
    }
    _global_opt = 'glob'

    class Config:
        allow_population_by_field_name = True
        extra = Extra.forbid

    def make_command(cls) -> str:
        """
        Make command.

        Returned cmd structure:
            `{main_cmd} {command_group} {glob_attrs} {inner_cmd} {inner_cmd_attrs}`

        Returns:
            command (str): created command
        """
        # clear
        cls.global_attrs = ''
        cls.inner_command_attrs = ''

        for f_name, f_value in cls.dict(exclude_none=True, exclude=cls._excluded_fields).items():

            cls._add_attributes(
                f_name, f_value,
            )

        command = '{main_cmd} {command_group} {glob_attrs} {inner_cmd} {inner_cmd_attrs}'.format(
            main_cmd=cls.main_command,
            command_group=cls.command_group,
            glob_attrs=cls.global_attrs.strip(),
            inner_cmd=cls.inner_command,
            inner_cmd_attrs=cls.inner_command_attrs.strip(),
        ).replace(
            '  ', ' ',
        )

        cmd_maker(
            msg='Created command: {0}'.format(command),
            severity=constants.SEV_INFO,
        )

        return command

    def _add_attributes(cls, field_name: str, field_value: Any):
        """
        Add attributes in-place based on attribute data type.

        Args:
            field_name (str): field name
            field_value (Any): field value

        """
        f_prop = cls.schema().get('properties').get(field_name)

        if f_prop.get('type', '') == 'array':

            for el in field_value:
                cls._add_attr(f_prop, el)

        elif f_prop.get('type', '') == 'boolean':

            if field_value:
                cls._add_attr(f_prop, None)

        else:
            cls._add_attr(f_prop, field_value)

    def _add_attr(cls, field_prop: dict, field_value: Any):
        """
        Add attribute in-place to related attribute group.

        Args:
            field_prop (dict): field property
            field_value (Any): field value

        """
        option = field_prop.get('option')
        if field_prop.get(cls._global_opt, False):
            cls.global_attrs = '{cmd} {option}{attr_divider}{field_value}'.format(
                cmd=cls.global_attrs,
                option=option if option else '',
                attr_divider=cls.attr_divider if (field_value and option) else '',
                field_value=field_value if field_value else '',
            ).strip()
        else:
            cls.inner_command_attrs = '{cmd} {option}{attr_divider}{field_value}'.format(
                cmd=cls.inner_command_attrs,
                option=option if option else '',
                attr_divider=cls.attr_divider if (field_value and option) else '',
                field_value=field_value if field_value else '',
            ).strip()
