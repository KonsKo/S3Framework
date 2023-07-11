"""Module to read res3 server log and append it to main framework log."""
import os
import time
from threading import Event, Thread
from typing import Callable, Optional

import constants
from helpers import output_handler

server_log = output_handler.OutputReaction(
    prefix='server_log',
    module_name=__name__,
)


current_data = {
    'repetitions': 0,
    'last_text': '',
    'last_method': '',
}


def connect_log(
    log_file: str, clean_old_data: bool = False, cb: Optional[Callable[[str], None]] = None,
):
    """
    Connect main framework log with log from different application in separate Thread.

    Args:
        log_file (str): server log file name
        clean_old_data (bool): preform cleaning log old data
        cb (Optional[Callable[[str], None]]): call-back function to process log row
    """
    thread = Thread(
        target=process_log_file,
        daemon=True,
        args=(
            log_file,
            clean_old_data,
            cb,
        ),
    )
    thread.start()


def process_log_file(
    log_file: str, clean_old_data: bool = False, cb: Optional[Callable[[str], None]] = None,
):
    """
    Process the log file.

    Function supposes to be invoked in another Thread.
    Infinite loop is going to be closed with thread closing.

    Args:
        log_file (str): server log file name
        clean_old_data (bool): preform cleaning log old data from file
        cb (Optional[Callable[[str], None]]): call-back function to process log row
    """
    if os.path.exists(log_file):

        if clean_old_data:
            with open(log_file, 'w') as start_lf:
                start_lf.truncate()

        with open(log_file, 'rt', encoding='latin-1') as lf:

            while True:

                line = lf.readline()

                if not line:
                    time.sleep(constants.SYNC_TIMEOUT)
                    continue

                # remove excessive new line char
                line = line.replace('\n', '')

                if cb:
                    cb(line)

    else:
        raise FileNotFoundError('Log file {0} does not exist.'.format(log_file))


def connect_s3_server_log(server_log_file: str, silent_event: Optional[Event] = None):
    """
    Connect main framework log with <>s3 server log.

    Args:
        server_log_file (str): server log file name
        silent_event (Optional[Event]): event to stop joining logs
    """
    def cb_server_log_line(line: str):
        """
        Do call-back to process log line after connecting.

        Args:
            line (str): server log line
        """
        should_be_skipped = predict_many_similar_lines(line) or silent_event.is_set()

        server_log(
            msg=line,
            severity=constants.SEV_EXTRA,
            no_log=should_be_skipped,
            no_screen=should_be_skipped,
        )

    # clean up old data
    # it is needed for local run, because watching of server log starts before server is started,
    # and old log (same file) has data from previous start
    connect_log(
        log_file=server_log_file,
        clean_old_data=True,
        cb=cb_server_log_line,
    )


QUANTITY_TO_SKIP = 3
SAME_PART_SIZE = 10


def predict_many_similar_lines(current_line: str) -> bool:
    """
    Predict similar log lines.

    Some requests can be invoked !!! thousands !!! times, hence, we will have
    a lot of similar log lines.
    Simple function to compare lines and disable printing out similar ones.

    Lines are being skipped, example:

    2022-10-12 17:36:05.748 1527229 INF 127.0.0.1:34266 PUT /finally3317/1 localhost:20 160 | 200 62
    2022-10-12 17:36:05.748 1527229 INF 127.0.0.1:34266 PUT /finally3317/2 localhost:20 160 | 200 62
    2022-10-12 17:36:05.748 1527229 INF 127.0.0.1:34266 PUT /finally3317/3 localhost:20 160 | 200 62
    2022-10-12 17:36:05.748 1527229 INF 127.0.0.1:34266 PUT /finally3317/4 localhost:20 160 | 200 62
    (Another types of lines such `connection closed` are not processed at all and always prints.)

    cur_text = current_line[7] - means part with url, from example it is `/finally3317/1`
    cur_method = current_line[6] - means part with method, from example it is `PUT`

    if `QUANTITY_TO_SKIP` lines in row with same `cur_method` and same `cur_text[:SAME_PART_SIZE]`,
    those lines considered are same, and they are going to be not printed out to log.

    From example, cur_method == `PUT` and cur_text[:10] == `/finally33`, hence, lines are
    considered as similar and are going to be skipped.

    Args:
        current_line (str): current line to check

    Returns:
        result (bool): line is considered similar or not
    """
    current_line = current_line.strip().split(' ')

    if len(current_line) > 7:
        cur_text = current_line[7]
        cur_method = current_line[6]
    else:
        return False

    if current_data.get('last_text', '') == cur_text[:SAME_PART_SIZE]:

        if current_data.get('last_method', '') == cur_method:

            if current_data.get('repetitions') >= QUANTITY_TO_SKIP:
                return True
            else:
                current_data['repetitions'] = current_data.get('repetitions', 0) + 1

        else:
            current_data['last_method'] = cur_method

    else:
        current_data['last_text'] = cur_text[:SAME_PART_SIZE]
        current_data['repetitions'] = 0
        current_data['last_method'] = cur_method

    return False
