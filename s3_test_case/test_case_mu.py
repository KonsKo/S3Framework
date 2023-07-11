"""Test case to process tests with multiple users instances."""
from clients.awsboto import AllBoto
from s3_test_case.test_case import S3AsyncTestCase


class MultipleUserTestCase(S3AsyncTestCase):
    """Test case class with multiple users."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.all_boto: AllBoto = AllBoto(server=cls.server)
