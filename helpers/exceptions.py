"""Exceptions module."""


class UnsupportedCommandNameError(Exception):
    """Error if invoking unsupported command."""


class InvokingCommandError(Exception):
    """Error if command invoking has error."""


class CreateCommandError(Exception):
    """Error if command creation has error."""


class TestDiscoveryFailedError(Exception):
    """Error if test discovery has failed."""


class FrameworkRunTimeError(Exception):
    """Framework run time error."""


class UnauthorizedError(Exception):
    """Error if authorization not happened."""


class BotoInitError(Exception):
    """Error if boto error happened."""


class LostConnectionError(Exception):
    """Error if connection with server was lost."""


class ServerIsDownError(AssertionError):
    """Error if server <>s3 is down."""


class ServerIsNotStarted(Exception):
    """Error if server could not start."""


class BucketExistsError(Exception):
    """Error, if bucket already exists."""
