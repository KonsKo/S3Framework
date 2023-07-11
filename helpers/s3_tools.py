"""Module to place different s3 related tools."""
import sys
from typing import Optional, Type

import constants
from clients.awsboto import Boto, BotoRealAws
from helpers.output_handler import OutputReaction


class AccountCleaner(object):
    """Class to clean/remove buckets/objects from S3 account."""

    reaction = OutputReaction(
        module_name=__name__,
        prefix='cleaner',
    )

    boto_class: Type[Boto] = BotoRealAws

    def __init__(self, profile_name: str):
        """
        Init class instance.

        Args:
            profile_name (str): settings profile name to perform actions from.
        """
        self._warning_for_action()
        self.boto = self.boto_class(profile_name=profile_name)

    async def clean(self, filter_buckets: Optional[list[str]] = None):
        """
        Clean account: delete objects, object versions, buckets.

        Args:
            filter_buckets (Optional[list[str]]): filter for choose particular buckets to delete
        """
        response_list_buckets = await self.boto.list_buckets()

        assert response_list_buckets.status == constants.HTTP_STAT.OK

        account_buckets = response_list_buckets.get('Buckets', [])
        account_buckets = [buck.get('Name') for buck in account_buckets]

        if account_buckets:

            if filter_buckets:
                account_buckets = [buck for buck in account_buckets if buck in filter_buckets]
                self.reaction(
                    msg='Bucket(s) {0} will be deleted.'.format(
                        account_buckets,
                    ),
                    severity=constants.SEV_INFO,
                )

            else:
                self.reaction(
                    msg='All buckets will be deleted from account.',
                    severity=constants.SEV_INFO,
                )

            await self.delete_buckets(buckets=account_buckets)

        else:
            self.reaction(
                msg='There is no buckets in account.',
                severity=constants.SEV_INFO,
            )

    async def delete_buckets(self, buckets: list):
        """
        Delete buckets from account.

        Args:
            buckets(list): list of buckets to delete
        """
        for bucket in buckets:

            response_delete_bucket_policy = await self.boto.delete_bucket_policy(
                Bucket=bucket,
            )
            assert response_delete_bucket_policy.status == constants.HTTP_STAT.NO_CONTENT

            response_bucket_versioning = await self.boto.get_bucket_versioning(
                Bucket=bucket,
            )
            assert response_bucket_versioning.status == constants.HTTP_STAT.OK

            if response_bucket_versioning.get('Status') in {'Enabled', 'Suspended'}:
                await self.delete_objects_versions(bucket=bucket)

            else:
                await self.delete_objects(bucket=bucket)

            response_delete_bucket = await self.boto.delete_bucket(
                Bucket=bucket,
            )

            assert response_delete_bucket.status == constants.HTTP_STAT.NO_CONTENT

            self.reaction(
                msg='Bucket={0} has been deleted'.format(
                    bucket,
                ),
                severity=constants.SEV_SYSTEM,
            )

    async def delete_objects_versions(self, bucket: str):
        """
        Delete all object versions from bucket.

        Args:
            bucket (str): bucket name to perform deletion of object versions
        """
        response_list_object_versions = await self.boto.list_object_versions(
            Bucket=bucket,
        )
        assert response_list_object_versions.status == constants.HTTP_STAT.OK

        object_versions = response_list_object_versions.get('Versions', [])

        if object_versions:
            await self._delete_objects_versions(
                bucket=bucket, object_versions=object_versions,
            )

        else:
            self.reaction(
                msg='Bucket={0} is empty: nothing to delete inside.'.format(
                    bucket,
                ),
                severity=constants.SEV_INFO,
            )

        # after deleting all object versions DeleteMarkers are left
        response_list_object_versions = await self.boto.list_object_versions(
            Bucket=bucket,
        )
        assert response_list_object_versions.status == constants.HTTP_STAT.OK

        delete_marker_versions = response_list_object_versions.get('DeleteMarkers', [])

        if delete_marker_versions:

            await self._delete_objects_versions(
                bucket=bucket, object_versions=delete_marker_versions,
            )

        else:
            self.reaction(
                msg='Bucket={0} is empty: there is no DeleteMarker inside.'.format(
                    bucket,
                ),
                severity=constants.SEV_INFO,
            )

    async def delete_objects(self, bucket: str):
        """
        Delete all objects from bucket.

        Args:
            bucket (str): bucket name to perform deletion of objects
        """
        response_list_objects = await self.boto.list_objects(
            Bucket=bucket,
        )
        assert response_list_objects.status == constants.HTTP_STAT.OK

        objects = response_list_objects.get('Contents', [])

        if objects:

            for obj in objects:
                obj_key = obj.get('Key')

                response_delete_object_version = await self.boto.delete_object(
                    Bucket=bucket,
                    Key=obj_key,
                )

                assert response_delete_object_version.status == constants.HTTP_STAT.NO_CONTENT

                self.reaction(
                    msg='Deleted object: Bucket={0}, Key={1}'.format(
                        bucket, obj_key,
                    ),
                    severity=constants.SEV_SYSTEM,
                )

        else:
            self.reaction(
                msg='Bucket={0} is empty: nothing to delete inside.'.format(
                    bucket,
                ),
                severity=constants.SEV_INFO,
            )

    async def _delete_objects_versions(self, bucket: str, object_versions: list[dict]):
        """
        Delete all object versions or delete markers from bucket.

        Args:
            bucket(str): bucket name to perform deletion of object versions
            object_versions (list[dict]): list of Versions/DeleteMarkers to delete,
        """
        for ver in object_versions:
            obj_key = ver.get('Key')
            obj_version = ver.get('VersionId')

            response_delete_object_version = await self.boto.delete_object(
                Bucket=bucket,
                Key=obj_key,
                VersionId=obj_version,
            )
            assert response_delete_object_version.status == constants.HTTP_STAT.NO_CONTENT

            self.reaction(
                msg='Deleted object: Bucket={0}, Key={1}, VersionId={2}'.format(
                    bucket, obj_key, obj_version,
                ),
                severity=constants.SEV_SYSTEM,
            )

    def _warning_for_action(self):
        self.reaction(
            msg="""\n\n
            ***************************************************

                WARNING!!!

                Next actions wil try to delete all buckets, objects, object versions
                from account.

                You will NOT be able to revert this action.

                If you wan to proceed - type `YES` bellow.

            ***************************************************
            """,
            severity=constants.SEV_WARNING,
        )

        confirmation = input('Type here:  ')
        if confirmation.lower() != 'yes':
            sys.exit(1)
