"""Request module."""
import contextlib
import ctypes
import json
import os
import socket
import ssl
import struct
from http.client import HTTPConnection, HTTPSConnection
from typing import Literal, Optional
from urllib.parse import ParseResult, urlparse

import constants
from helpers import framework, output_handler

request_reaction = output_handler.OutputReaction(
    module_name=__name__,
    prefix='request',
)

SOFT = 'soft'  # shutdown(socket.SHUT_RDWR)
HARD = 'hard'  # SO_LINGER in connect + shutdown(socket.SHUT_RDWR)
HARD2 = 'hard2'  # SO_LINGER in connect + SO_LINGER in send
                 # + shutdown(socket.SHUT_RDWR)
                 # Needed to diversify races between send and shutdown.
HARDEST = 'hardest'  # connect(AF_UNSPEC)
CLOSE_SOCKET_MODES = (SOFT, HARD, HARD2, HARDEST)


class SockaddrIn(ctypes.Structure):
    _fields_ = [('sa_family', ctypes.c_ushort),
                ('sin_port', ctypes.c_ushort),
                ('sin_addr', ctypes.c_byte * 4),
                ('__pad', ctypes.c_byte * 8)]


# SO_LINGER with interval = 0 first tries to close the connection gracefully,
# so it's not really hard reset.
# The hardest way is connect(AF_UNSPEC) (Linux-only).
# The behavior is poorly-documented:
# https://stackoverflow.com/questions/46264404/how-can-i-reset-a-tcp-socket-in-python/54065411#54065411
# https://stackoverflow.com/questions/38507586/reset-a-tcp-socket-connection-from-application
def hardest_close_connection(conn_fd: int) -> None:
    addr = SockaddrIn()
    addr.sa_family = ctypes.c_ushort(socket.AF_UNSPEC)
    addrlen = ctypes.c_int(ctypes.sizeof(addr))

    libc = ctypes.CDLL('libc.so.6', use_errno=True)
    res = libc.connect(conn_fd, ctypes.byref(addr), addrlen)
    if res == -1:
        raise OSError(ctypes.get_errno(), os.strerror(ctypes.get_errno()))


def pretty_print_sending_data(data):
    """
    Print out data to console.

    Args:
        data: data to print out

    """
    request_reaction(
        msg='Sent request data:\nSTART\n{0}\nEND'.format(
            data.decode(),
        ),
        severity=constants.SEV_INFO,
    )


class UniResponse(object):
    """
    Custom Response class to just keep data in one structure.

    Status equals to 0 (zero) means `OSError: Bad file descriptor` - error to get response
    """

    def __init__(self, status, body, headers):
        self.status = status
        self.body = body
        self.headers = headers
        self.tls_version = None

    def load_text(self) -> str:
        """
        Decode body.

        Returns:
            decoded_body (str): decoded body

        """
        if self.body:
            return self.body.decode()

    def load_json(self) -> dict:
        """
        Decode json body.

        Returns:
            decoded_body (dict): decoded body

        """
        if self.body:
            try:
                return json.loads(self.body)
            except json.decoder.JSONDecodeError:
                return {}


def build_connection_class(scheme: str = 'https'):
    """
    Dynamically return base connection class.

    Args:
        scheme (str): url scheme

    Returns:
        connection class

    """
    if scheme == 'https':
        base_class = HTTPSConnection
    elif scheme == 'http':
        base_class = HTTPConnection
    else:
        raise request_reaction(
            msg='Parameter `scheme` must be either `http` or `https`, got `{0}`'.format(scheme),
            severity=constants.SEV_ERROR,
            returned_exception=ValueError,
        )

    https_only_kwargs = ('key_file', 'cert_file', 'context', 'check_hostname')

    class UniConnection(base_class):
        """Connection class as for http as for https."""

        def __init__(
            self,
            ignored_headers: Optional[list] = None,
            ignored_data: Optional[list] = None,
            extra_headers: Optional[tuple] = None,
            detailed_info: bool = True,
            close_socket_after_send: Optional[Literal['soft', 'hard', 'hardest']] = None,
            *args,
            **kwargs,
        ):
            # remove https related kwargs if http
            if base_class == HTTPConnection:
                [kwargs.pop(element) for element in https_only_kwargs if element in kwargs.keys()]

            super().__init__(*args, **kwargs)
            self.detailed_info = detailed_info
            self.ignored_headers = ignored_headers if ignored_headers else []
            self.ignored_data = ignored_data if ignored_data else []
            self.extra_headers = extra_headers if extra_headers else ()

            self.close_socket_after_send = None
            if close_socket_after_send:
                if close_socket_after_send in CLOSE_SOCKET_MODES:
                    self.close_socket_after_send = close_socket_after_send
                else:
                    request_reaction(
                        msg='Allowed values for close socket are: `{0}`,you provided `{1}`.'.format(
                            ', '.join(CLOSE_SOCKET_MODES),
                            close_socket_after_send,
                        ),
                        severity=constants.SEV_ERROR,
                    )

        def connect(self):
            rslt = super().connect()

            request_reaction(
                msg='Socket info: {0}.'.format(
                    self.sock.getsockname(),
                ),
                severity=constants.SEV_INFO,
            )

            # Moved setsockopt here to minimize amount of calls between
            # send and shutdown.
            if self.close_socket_after_send:
                if self.close_socket_after_send in {HARD, HARD2}:
                    # Convert gracefull shutdown into an immediate reset.
                    self.sock.setsockopt(
                        socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0),
                    )

            return rslt

        def send(self, data) -> None:
            # ignore any request data if it needed,
            # for example: b'0\r\n\r\n' - end chunked transfer
            if data in self.ignored_data:
                request_reaction(
                    msg='Data `{0}` will be ignored.'.format(
                        data,
                    ),
                    severity=constants.SEV_INFO,
                )

            else:
                if framework.structure.config.get('show_requests_info', False):
                    if self.detailed_info:
                        pretty_print_sending_data(data)
                super().send(data)

            # to test purposes, to imitate connection closing right after data have been sent
            # Do not put any additional code between send() and shutdown().
            if self.close_socket_after_send:
                if self.close_socket_after_send == HARDEST:
                    hardest_close_connection(self.sock.fileno())
                else:
                    # for HARD the SO_LINGER is set withing connect()
                    if self.close_socket_after_send == HARD2:
                        # notify peer that we do not want to receive anything
                        self.sock.setsockopt(
                            socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0),
                        )
                    self.sock.shutdown(socket.SHUT_RDWR)

        # overwrite method to ignore some headers if it needed
        def putheader(self, header, *values):
            if header not in self.ignored_headers:
                super().putheader(header, *values)
            else:
                request_reaction(
                    msg='Header: {0} with value(s) `{1}` will be ignored.'.format(
                        header, ' '.join(values),
                    ),
                    severity=constants.SEV_INFO,
                )

        # overwrite method to add some extra header(s),
        # method `endheaders` invoked in the end of send request - best place to add headers,
        # we want to add this method to test behavior for many headers with same field names,
        # library does not allow it: headers are processed as set (no duplicates).
        def endheaders(self, message_body=None, *, encode_chunked=False):
            for extra_header in self.extra_headers:
                self.putheader(
                    extra_header[0],
                    extra_header[1],
                )
            super().endheaders(message_body=message_body, encode_chunked=encode_chunked)

        def create_response_only(self):
            return self.response_class(self.sock, method=self._method)

    return UniConnection


S3HTTPConnection = build_connection_class('http')
S3HTTPSConnection = build_connection_class('https')


def get_connection_class(scheme: str = 'https'):
    """
    Return named connection class that was created dynamically.

    Args:
        scheme (str): url scheme

    Returns:
        connection: connection class
    """
    if scheme == 'https':
        return S3HTTPSConnection
    elif scheme == 'http':
        return S3HTTPConnection


def make_request(
    url: str,
    method: str = 'GET',
    headers: Optional[dict] = None,
    body: Optional[bytes] = None,
    ca_cert: Optional[str] = None,
    no_check_cert: bool = False,
    detailed_info: bool = True,
    ignored_headers: Optional[list] = None,
    encode_chunked: bool = False,
    ignored_data: Optional[list] = None,
    extra_headers: Optional[tuple] = None,
    check_for_status: bool = False,
    mask_connection_reset: bool = True,
    close_socket_after_send: Optional[Literal['soft', 'hard', 'hardest']] = None,
) -> UniResponse:
    """
    Make request.

    ignored_headers: some headers such 'Content-Length' are generated by library, if you want to
        ignore any of them fill up this parameter.
        That parameter has privilege over `extra_headers`.

    extra_headers: headers duplicating is prohibited, if you want to have several headers with
        same field name fill up this parameter.

    Args:
        body (Optional[bytes]): request body
        url (str): url to process
        method (str): method to process
        headers (Optional[dict]): http headers
        ca_cert (Optional[str]): ssl certificate
        no_check_cert (bool): do request with no cert validation
        detailed_info (bool): print out detailed request data
        ignored_headers (Optional[list]): name of headers to do not send to server
        encode_chunked (bool): to enable chunked encoding
        ignored_data (Optional[list]): request data NOT to send to server
        extra_headers (Optional[tuple]): extra headers
        check_for_status (bool): flag to check for status only
        mask_connection_reset (bool): flag to mask reset of connection
        close_socket_after_send (Optional[Literal['soft', 'hard', 'hardest']]): \
            close socket after data have been sent (hard - we do not want to receive anything)

    Returns:
        response (PlainResponse): response

    Raises:
        boto_reaction: if error happened

    """
    parsed_url: ParseResult = urlparse(url)

    ssl_ctx = None
    if parsed_url.scheme == 'https':
        if ca_cert:
            ssl_ctx = ssl.create_default_context(cafile=ca_cert)
        else:
            ssl_ctx = ssl._create_unverified_context()

    if parsed_url.scheme == 'http':
        no_check_cert = True

    connection = get_connection_class(scheme=parsed_url.scheme)(
        host=parsed_url.netloc,
        timeout=constants.HTTP_TIMEOUT,
        context=ssl_ctx if ssl_ctx else None,
        check_hostname=not no_check_cert,
        detailed_info=detailed_info,
        ignored_headers=ignored_headers,
        ignored_data=ignored_data,
        extra_headers=extra_headers,
        close_socket_after_send=close_socket_after_send,
    )

    request_reaction(
        msg='Request: {0} {1}, headers {2}, body {3}'.format(
            method, url, headers, bool(body),
        ),
        severity=constants.SEV_INFO,
    )

    uri = '{0}?{1}'.format(parsed_url.path, parsed_url.query) if parsed_url.query else parsed_url.path
    response = None

    try:
        connection.request(
            url=uri,
            method=method,
            headers=headers if headers else {},
            body=body,
            encode_chunked=encode_chunked,
        )

        # used to get 100-Continue response in test cases
        if check_for_status:
            response = connection.create_response_only()
            _, status, _ = response._read_status()
            rslt = UniResponse(status, '', '')

            with contextlib.suppress(AttributeError):
                rslt.tls_version = connection.sock.version()

        else:
            response = connection.getresponse()
            status = response.status
            rslt = UniResponse(status, response.read(), response.headers)

            with contextlib.suppress(AttributeError):
                rslt.tls_version = connection.sock.version()

    # TimeoutError is child of OSError, we need to split them
    except TimeoutError:
        raise

    except (ConnectionResetError, ConnectionRefusedError):
        if not mask_connection_reset:
            raise
        # make empty response because we need return something
        rslt = UniResponse(0, '', '')

    finally:
        if response:
            response.close()
        connection.close()
        del connection

    if response and not response.isclosed():
        raise request_reaction(
            msg='Connection is not closed.',
            severity=constants.SEV_ERROR,
            returned_exception=ConnectionError,
        )

    request_reaction(
        msg='Result: status - {0}, body - {1}, headers - {2}.'.format(
            rslt.status, rslt.body, rslt.headers,
        ),
        severity=constants.SEV_INFO,
    )

    return rslt
