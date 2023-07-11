"""
S3browser client module.

It is Windows GUI application, working under `wine`.

Free version of app allows to have two accounts only: we will have one for running with server
under VFS and one for <>FS.

pyautogui does not work with Wayland.

TODO Work on client was stopped.
TODO Module left unfinished as is in case to save work.
"""
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

import constants
from clients.wine import Wine
from helpers import cmd, log_connector, output_handler, utils

# import pyautogui


# pause after every action
pyautogui.PAUSE = 0.5

S3b_reaction = output_handler.OutputReaction(
    module_name=__name__,
    prefix='s3br',
)
S3b_log = output_handler.OutputReaction(
    module_name=__name__,
    prefix='s3br_log',
)

TCoord = tuple[int, int]


@dataclass
class AccountNames(object):
    """
    Account names list.

    Free version of app allows to have two accounts only.
    """

    default_vfs: str = 'default_vfs'
    default_fs: str = 'default_<>fs'


# TODO more items will be added later
@dataclass
class Buttons(object):
    """Buttons list."""

    alt: str = 'alt'
    f10: str = 'f10'


# TODO more items will be added later
@dataclass
class MenuCoordinates(object):
    """Coordinates list for S3browser menu in case when app expanded to full screen."""

    accounts: TCoord = (100, 75)


# TODO more items will be added later
@dataclass()
class MenuAccounts(object):
    """Menu: Accounts."""

    manage_accounts: str = 'Manage accounts'


# TODO more items will be added later
@dataclass
class MenuStructure(object):
    """WHole menu structure for S3browser."""

    accounts = MenuAccounts


class S3browser(object):
    """
    Client class.

    S3browser is Windows application and working under `wine`.
    """

    app_location = 'S3 Browser/s3browser-win32.exe'
    app_name = 's3browser-win32.exe'
    wine_class = Wine
    reaction = S3b_reaction
    reaction_log = S3b_log

    menu_coordinates = MenuCoordinates
    menu_structure = MenuStructure
    buttons = Buttons

    DELAY = 2

    TIME_FORMAT = '%Y-%m-%d  %H:%M:%S'

    def __init__(self, acc_type: Literal['vfs', '<>fs'] = 'vfs'):
        """Init class instance."""
        self.started_at: Optional[datetime] = None
        self.log_buffer: list[str] = []
        self.wine_instance = self.wine_class()
        self.app_proc: Optional[cmd.ProcessPopenType] = None
        self.acc_name = constants.S3B_ACC_VFS if acc_type == 'vfs' else constants.S3B_ACC_SERVERFS

    def start(self):
        """Start app and expand to full screen."""
        self.app_proc = self.wine_instance.start(self.app_location)

        time.sleep(self.DELAY)  # time to allow to app starts

        if self.is_running():
            self.started_at = datetime.now().replace(microsecond=0)
            self.make_full_screen()

        else:
            raise self.reaction(
                msg='App is not running.',
                severity=constants.SEV_EXCEPTION,
                returned_exception=RuntimeError,
            )

        self.reading_log()

    def is_running(self) -> bool:
        """
        Check app is running.

        Returns:
            status (bool): app status
        """
        if self.app_proc:
            return self.app_proc.is_running() and self.app_proc.status() != 'zombie'

        return False

    def stop(self):
        """Stop app."""
        self.wine_instance.stop()

    # TODO login works if account data is already filled and account name is matched
    def login(self):
        """Do login."""
        self._move_mouse(
            *self.menu_coordinates.accounts,
        )
        self._click_mouse_cur_pos()

        # choose particular account
        self._typewrite(
            self.acc_name,
        )

        if self.is_in_log_buffer('Successfully assigned account {0}'.format(self.acc_name)):
            self.reaction(
                msg='Successful login.',
                severity=constants.SEV_INFO,
            )

        else:
            raise self.reaction(
                msg='Something went wrong with logging: check log.',
                severity=constants.SEV_EXCEPTION,
                returned_exception=RuntimeError,
            )

    def make_full_screen(self):
        """Expand app to full screen."""
        # click mouse to make app as active
        self._click_mouse_cur_pos()
        # combination to expand app to full screen.
        self._push_keys(
            self.buttons.alt, self.buttons.f10,
        )

    def is_in_log_buffer(self, search: str) -> bool:
        """
        Check that log buffer contains requested data.

        Args:
            search (str): data to search.

        Returns:
            status (bool): is requested data in buffer
        """
        for row in self.log_buffer:
            if row.find(search) >= 0:
                return True

        return False

    def reading_log(self):
        """
        Read app log data into buffer.

        Log file is created for every day of running, and it formatted:
        `s3browser-win32-YEAR-MONTH-DAY-log.txt`
        """
        log_file_name = 's3browser-win32-{year}-{month:02d}-{day:02d}-log.txt'.format(
            year=datetime.now().year,
            month=datetime.now().month,
            day=datetime.now().day,
        )
        log_path = os.path.join(
            constants.S3B_LOG_LOCATION,
            log_file_name,
        )

        log_path = utils.expand_user_path(log_path)

        log_connector.connect_log(
            log_file=log_path,
            cb=self._add_to_log_buffer,
        )

    def _move_mouse(self, x_pos: int, y_pos: int):
        """
        Move mouse to position x-y.

        Args:
            x_pos (int): position by X-axis to move mouse
            y_pos (int): position by Y-axis to move mouse
        """
        max_x, max_y = pyautogui.size()
        min_x, min_y = 1, 1

        if x_pos < min_x:
            x_pos = min_x
        elif x_pos > max_x:
            x_pos = max_x
        if y_pos < min_y:
            y_pos = min_y
        elif y_pos > max_x:
            y_pos = max_y

        self.reaction(
            msg='Moving cursor to x: {0}, y: {1}.'.format(x_pos, y_pos),
            severity=constants.SEV_INFO,
        )

        try:
            pyautogui.moveTo(x_pos, y_pos)
        except Exception:
            raise self.reaction(
                msg='Something went wrong with changing cursor position.',
                severity=constants.SEV_EXCEPTION,
                returned_exception=RuntimeError,
            )

    def _click_mouse_cur_pos(self):
        """Click mouse to current position."""
        current_x, current_y = pyautogui.position()
        self.reaction(
            msg='Mouse clicking by position x: {0}, y: {1}.'.format(current_x, current_y),
            severity=constants.SEV_INFO,
        )

        try:
            pyautogui.click()
        except Exception:
            raise self.reaction(
                msg='Something went wrong with cursor clicking.',
                severity=constants.SEV_EXCEPTION,
                returned_exception=RuntimeError,
            )

    def _push_keys(self, *keys: str):
        """
        Push keyboard keys together.

        Args:
            keys (str): keys to push
        """
        self.reaction(
            msg='Next keys will be pushed: {0}.'.format(keys),
            severity=constants.SEV_INFO,
        )

        try:
            pyautogui.hotkey(*keys)
        except Exception:
            raise self.reaction(
                msg='Something went wrong with key pushing.',
                severity=constants.SEV_EXCEPTION,
                returned_exception=RuntimeError,
            )

    def _typewrite(self, message: str):
        """
        Perform a keyboard key press down, followed by a release.

        It may be used as kind-of-short-cuts.

        Args:
             message (str): message to perform.
        """
        self.reaction(
            msg='Next message will be typewritten (possibly used as short-cut): {0}.'.format(
                message,
            ),
            severity=constants.SEV_INFO,
        )

        try:
            pyautogui.typewrite(message)
        except Exception:
            raise self.reaction(
                msg='Something went wrong with typewriting.',
                severity=constants.SEV_EXCEPTION,
                returned_exception=RuntimeError,
            )

    def _add_to_log_buffer(self, row: str):
        """
        Add data to log buffer.

        Line example: '[I] [2023-06-16 09:26:07] Application started: ...'

        Args:
            row (str): data to add to buffer
        """
        date_time_search = re.search(
            pattern=r'\[I\]\s\[(.+)\]',
            string=row,
        )
        if date_time_search:
            date_time = date_time_search.group(1)

            # ignore data before app was started
            if datetime.strptime(date_time, self.TIME_FORMAT) >= self.started_at:
                self.log_buffer.append(row)

                self.reaction_log(
                    msg=row,
                    severity=constants.SEV_EXTRA,
                )
