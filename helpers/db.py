"""SQLite module."""
import os
import sqlite3
import traceback
from contextlib import suppress
from pathlib import Path
from typing import Any, Callable, Optional

from pydantic import BaseModel, Field

import constants
from helpers import output_handler

db_reaction = output_handler.OutputReaction(
    module_name=__name__,
    prefix='db',
)

DB_DIR = '.framework'


def supress_exception(method: Callable) -> Callable:
    """
    Supress exception and print out traceback.

    Decorator.

    We do not want to get fail while working with DB. Exception will be caught and
    just printed out to console.

    Args:
        method (Callable): method to supress exceptions for (target for decoration).

    Returns:
        method_result (Callable): result of decorated method

    """
    def wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception as exc:
            # print out traceback to stdout for more information
            traceback.print_exception(exc)
            db_reaction(
                msg='Error occurred during invoking of `{0}`.'.format(method),
                severity=constants.SEV_ERROR,
            )

    return wrapper


class Table(BaseModel):
    """Base Table class."""

    @classmethod
    def field_names(cls, with_sql_types: bool = False) -> tuple:
        """
        Retrieve filed names.

        Args:
            with_sql_types (bool): also, retrieve sql data types.

        Returns:
            fields (tuple): fields name
        """
        field_names = []

        for field in cls.__fields__:

            if with_sql_types:
                field_with = '{0} {1}'.format(
                    field,
                    cls.schema().get('properties').get(field).get('sql_type'),
                )
                field_names.append(field_with)

            else:
                field_names.append(field)

        return tuple(field_names)

    @classmethod
    def table_name(cls) -> str:
        """
        Retrieve Table name.

        Returns:
            name (str): table name
        """
        return cls.schema().get('title')


class IgnoredTests(Table):
    """Table: IgnoredTests."""

    test_id: str = Field(sql_type='TEXT NOT NULL UNIQUE')
    num_of_runs: int = Field(sql_type='INTEGER DEFAULT 0')
    num_of_fails: int = Field(sql_type='INTEGER DEFAULT 0')
    num_of_runs_fs: int = Field(sql_type='INTEGER DEFAULT 0')
    num_of_fails_fs: int = Field(sql_type='INTEGER DEFAULT 0')

    num_of_checks: int = Field(sql_type='INTEGER DEFAULT 0')


class BaseDB(object):
    """Base SQLite DB class representation."""

    def __init__(self, config: dict):
        """
        Init class instance.

        Args:
            config (dict): db-related config
        """
        self.config = config
        self.db_location = self.load_db_location()
        self.connection = self.create_connection()
        self.cursor: Optional[sqlite3.Cursor] = None

    @supress_exception
    def load_db_location(self) -> str:
        """
        Load DB file location.

        By default, DB file is located inside `HOME/DB_DIR/self.config.get('db_name')`.
        But it is possible to set up absolute path to DB in `self.config.get('db_name')`

        Returns:
            location (str): DB location as path.
        """
        if os.path.isabs(self.config.get('db_name', '')):
            return self.config.get('db_name')

        db_inner_dir = os.path.join(
            Path.home(),
            DB_DIR,
        )

        if os.path.exists(db_inner_dir):
            db_reaction(
                msg='DB directory {0} exists'.format(db_inner_dir),
                severity=constants.SEV_INFO,
            )
        else:
            os.mkdir(db_inner_dir)
            db_reaction(
                msg='DB directory was created: {0}'.format(db_inner_dir),
                severity=constants.SEV_INFO,
            )

        return os.path.join(
            db_inner_dir,
            self.config.get('db_name'),
        )

    @supress_exception
    def create_connection(self) -> sqlite3.Connection:
        """
        Create DB connection.

        Returns:
            connection (sqlite3.Connection): DB connection
        """
        try:
            conn = sqlite3.connect(self.db_location)
            db_reaction(
                msg='Connection to {0} has been established. SQLite library version {1}'.format(
                    self.db_location, sqlite3.version,
                ),
                severity=constants.SEV_INFO,
            )
            return conn

        except Exception as exc:
            db_reaction(
                msg=['Failed to create db connection.', str(exc)],
                severity=constants.SEV_WARNING,
            )

    @supress_exception
    def create_cursor(self):
        """Create DB cursor."""
        if self.connection:

            try:
                self.cursor = self.connection.cursor()

            except Exception as exc:
                db_reaction(
                    msg=['Failed to create db cursor.', str(exc)],
                    severity=constants.SEV_WARNING,
                )

        else:
            db_reaction(
                msg='Failed to create db cursor: there is no connection.',
                severity=constants.SEV_ERROR,
            )

    @supress_exception
    def stop(self):
        """Close cursor and connection."""
        try:
            if self.cursor:
                self.cursor.close()

            else:
                db_reaction(
                    msg='Cursor has not been closed: did not exist.',
                    severity=constants.SEV_WARNING,
                )

            if self.connection:
                self.connection.close()
                db_reaction(
                    msg='Connection has been closed.',
                    severity=constants.SEV_INFO,
                )
            else:
                db_reaction(
                    msg='Connection has not been closed: did not exist.',
                    severity=constants.SEV_WARNING,
                )

        except Exception as exc:
            db_reaction(
                msg=['Failed to close cursor and connection.', str(exc)],
                severity=constants.SEV_ERROR,
            )

    def select(self, table: str, fields: str = '*', where: Optional[str] = None) -> list:
        """
        Select data from Table.

        Args:
            table (str): table name to select data from.
            fields (str): fields name(s) to select (separate by comma if many)
            where (Optional[str]): condition

        Returns:
            result (list): result of select
        """
        query = 'SELECT {fields} FROM {table} {where_clause};'.format(
            fields=fields,
            table=table,
            where_clause='WHERE {where}'.format(where=where) if where else '',
        )

        try:
            return self.execute_sql(query=query)

        except Exception as exs:
            db_reaction(
                msg=['DB error: `select`', str(exs)],
                severity=constants.SEV_ERROR,
            )
            return []

    def update(self, table: str, set_values: str, where: Optional[str] = None):
        """
        Update data in Table.

        Args:
            table (str): table name to update data in.
            set_values (str): value to change
            where (Optional[str]): condition
        """
        query = 'UPDATE {table} SET {set_values} {where_clause};'.format(
            table=table,
            set_values=set_values,
            where_clause='WHERE {where}'.format(where=where) if where else '',
        )

        self.execute_sql(query=query)

    def delete(self, table: str, where: Optional[str] = None):
        """
        Update data from Table.

        Args:
            table (str): table name to update data in.
            where (Optional[str]): condition
        """
        query = 'DELETE FROM {table} {where_clause};'.format(
            table=table,
            where_clause='WHERE {where}'.format(where=where) if where else '',
        )

        self.execute_sql(query=query)

    def execute_sql(self, query: str, parameters: Optional[dict] = None) -> list:
        """
        Execute SQL query.

        Args:
            query (str): SQL query to execute.
            parameters (Optional[dict]): values to bind to placeholders in sql

        Returns:
            result (list): all (remaining) rows of a query result.
        """
        if not parameters:
            parameters = {}

        if self.connection:

            self.create_cursor()

            # print out query
            # db_reaction(
            #    msg=query,
            #    severity=constants.SEV_INFO,
            # )

            try:
                query_result = self.cursor.execute(query, parameters)

                db_reaction(
                    msg='Modified rows: `{0}`'.format(self.cursor.rowcount),
                    severity=constants.SEV_INFO,
                )

                self.connection.commit()

                return query_result.fetchall()

            finally:
                self.cursor.close()

        db_reaction(
            msg='There is no connection.',
            severity=constants.SEV_WARNING,
        )

    @supress_exception
    def create_table(self, table: str, fields_with_types: Any):
        """
        Create Table if not exists.

        Args:
            table (str): table name to create
            fields_with_types (Any): fields to create with name and sql type.
        """
        fields_with_types = str(fields_with_types)

        if self.is_table_exist(table):
            db_reaction(
                msg='Creating table: `{0}` is already exists.'.format(table),
                severity=constants.SEV_SYSTEM,
            )

        else:
            query = 'CREATE TABLE IF NOT EXISTS {table} {fields_and_types};'.format(
                table=table,
                fields_and_types=fields_with_types,
            )
            self.execute_sql(query=query)

            if self.is_table_exist(table):
                db_reaction(
                    msg='Creating table: `{0}` is created successfully.'.format(table),
                    severity=constants.SEV_SYSTEM,
                )

    def is_table_exist(self, table: str) -> bool:
        """
        Check if table is existed.

        Args:
            table (str): table name to check

        Returns:
            status (bool): existence status of table
        """
        # request to check table is existed by the docs
        response = self.execute_sql('SELECT name FROM sqlite_master;')

        if response:

            # `response` is list[tuple], make it float list
            response = [table_name for el in response for table_name in el]

            is_existed = table in response

            db_reaction(
                msg='Check table existence: {0} -> {1}'.format(response, is_existed),
                severity=constants.SEV_INFO,
            )
            return is_existed


class DB(BaseDB):
    """DB class representation. Table Model is based on pydantic."""

    table_ignored_tests = IgnoredTests

    NUM_OF_CHECKS_REQUIRED = 20

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.create_table_ignored_tests()

    @supress_exception
    def create_table_ignored_tests(self):
        """Create table IgnoredTests if not exists."""
        fields_and_types = str(
            self.table_ignored_tests.field_names(with_sql_types=True),
        )
        fields_and_types = fields_and_types.replace('\'', '')

        self.create_table(
            table=self.table_ignored_tests.table_name(),
            fields_with_types=fields_and_types,
        )

    @supress_exception
    def select_tests(self, where: Optional[str] = None) -> list[IgnoredTests]:
        """
        Select rows from table IgnoredTests.

        Args:
            where (Optional[str]): condition

        Returns:
            rows (list[IgnoredTests]): selected data
        """
        rows = []

        for row in self.select(table=self.table_ignored_tests.table_name(), where=where):

            with suppress(Exception):
                rows.append(
                    self.table_ignored_tests(
                        test_id=row[0],
                        num_of_runs=row[1],
                        num_of_fails=row[2],
                        num_of_runs_fs=row[3],
                        num_of_fails_fs=row[4],
                        num_of_checks=row[5],
                    ),
                )

        return rows

    @supress_exception
    def write_ignored_test(
        self,
        test_id: str,
        num_of_runs_incr: int = 0,
        num_of_fails_incr: int = 0,
        num_of_runs_fs_incr: int = 0,
        num_of_fails_fs_incr: int = 0,
        num_of_checks_incr: int = 0,
    ):
        """
        Do UPSERT.

        UPSERT is a special syntax addition to INSERT that causes the INSERT to behave as an UPDATE
        or a no-op if the INSERT would violate a uniqueness constraint.

        Args:
            test_id (str): id of the test
            num_of_runs_incr (int): increment value for field `num_of_runs`
            num_of_fails_incr (int): increment value for field `num_of_fails`
            num_of_runs_fs_incr (int): increment value for field `num_of_runs_fs`
            num_of_fails_s_incr (int): increment value for field `num_of_fails_fs`
            num_of_checks_incr (int): increment value for field `num_of_checks_incr`
        """
        query_upsert = """
            INSERT INTO {table} {fields}
            VALUES ({values_mapping})
            ON CONFLICT({on_conflict})
            DO UPDATE
            SET
            num_of_runs=num_of_runs+:num_of_runs_incr,
            num_of_fails=num_of_fails+:num_of_fails_incr,
            num_of_runs_fs=num_of_runs_fs+:num_of_runs_fs_incr,
            num_of_fails_fs=num_of_fails_fs+:num_of_fails_fs_incr,
            num_of_checks=num_of_checks+:num_of_checks_incr;
        """
        values_to_insert = (
            test_id,
            num_of_runs_incr,
            num_of_fails_incr,
            num_of_runs_fs_incr,
            num_of_fails_fs_incr,
            num_of_checks_incr,
        )
        field_names = self.table_ignored_tests.field_names()

        query_upsert = query_upsert.format(
            table=self.table_ignored_tests.table_name(),
            fields=field_names,
            values_mapping=', '.join(
                (':{0}'.format(field_name) for field_name in field_names),
            ),
            on_conflict='test_id',
        )

        # pass values through the `parameters` of `execute`, by the docs
        query_parameters = dict(zip(field_names, values_to_insert))
        query_parameters.update(
            {
                'num_of_runs_incr': num_of_runs_incr,
                'num_of_fails_incr': num_of_fails_incr,
                'num_of_runs_fs_incr': num_of_runs_fs_incr,
                'num_of_fails_fs_incr': num_of_fails_fs_incr,
                'num_of_checks_incr': num_of_checks_incr,
            },
        )

        self.execute_sql(query=query_upsert, parameters=query_parameters)

    @supress_exception
    def clean_ignored_test(self, still_ignored_tests: tuple):
        """
        Clean IgnoredTests.

        Remove tests are not in ignored anymore if `num_of_checks` >= `NUM_OF_CHECKS_REQUIRED`.
        Update tests are not in ignored anymore if `num_of_checks` < `NUM_OF_CHECKS_REQUIRED`.
        It is kind-of-protection to not remove test, running from user-related-branch(not main).

        Args:
            still_ignored_tests (tuple): tuple of ignored tests
        """
        self.update(
            table=self.table_ignored_tests.table_name(),
            set_values='num_of_checks=num_of_checks+1',
            where='test_id NOT IN {still_ignored_tests} and num_of_checks < {limit}'.format(
                still_ignored_tests=still_ignored_tests,
                limit=self.NUM_OF_CHECKS_REQUIRED,
            ),
        )

        self.delete(
            table=self.table_ignored_tests.table_name(),
            where='test_id NOT IN {still_ignored_tests} and num_of_checks >= {limit}'.format(
                still_ignored_tests=still_ignored_tests,
                limit=self.NUM_OF_CHECKS_REQUIRED,
            ),
        )
