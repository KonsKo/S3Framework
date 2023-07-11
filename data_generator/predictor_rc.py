"""Module to predict http return code."""
import http
from typing import Optional

import constants


class HttpCodePredictorByACL(object):

    put_object_success_canned_acl = ('public-read-write',)
    put_object_success_direct_acl = ('GrantFullControl', 'GrantWrite',)

    def predict_http_code_for_put_object(
        self, by_acl: dict, for_who: Optional[str] = None, owner_id: Optional[str] = None,
    ):
        return self.predict_http_code(
            by_acl=by_acl,
            for_who=for_who,
            owner_id=owner_id,
            success_canned_acl=self.put_object_success_canned_acl,
            success_direct_acl=self.put_object_success_direct_acl,
            expected_success_code=http.HTTPStatus.OK,
        )

    def predict_http_code(
        self,
        by_acl: dict,
        success_canned_acl: tuple,
        success_direct_acl: tuple,
        for_who: Optional[tuple[str]] = None,
        owner_id: Optional[str] = None,
        expected_success_code: int = http.HTTPStatus.OK,
        expected_bad_code: int = http.HTTPStatus.FORBIDDEN,
    ) -> int:
        """
        Predict returned http code.

        ACl is `private-by-default`, if one of grants gives access - got access.

        `for_who` - should be determined as regular ACL `emailAddress="xyz@amazon.com"`

        Limitations for now:
            - owner determines by id only
            - for_who should be tuple, if it has only one element, it should be represented
                as tuple of string (example: ('id="123..."',) - as tuple of string)
            - NO predictions by e-mail

        Args:
            by_acl (dict): ACL to predict code
            for_who (Optional[tuple[str]]): predict for particular user, if empty - owner
            owner_id (Optional[str]): owner id
            success_canned_acl (tuple): canned acl(s) names to allow successfully grants
            success_direct_acl (tuple): direct acl(s) names to allow successfully grants
            expected_success_code (int): expected http code to successfully grants
            expected_bad_code (int): expected http code to NOT successfully grants

        Returns:
            http_code (int): returned http code
        """
        # if not for_who - owner
        if not for_who:
            return expected_success_code

        # if owner id is present in for_who - success
        if owner_id and 'id="{0}"'.format(owner_id) in for_who:
            return expected_success_code

        for acl_k, acl_v in by_acl.items():

            # write for everyone
            if acl_k == 'ACL':
                if acl_v in success_canned_acl:
                    return expected_success_code

            # check direct grants for user
            elif acl_k in success_direct_acl:

                for who in for_who:
                    if acl_v == who:
                        return expected_success_code

                    # we suppose, if `for_who` contains `id` then user is AuthenticatedUsers
                    elif acl_v == 'uri="{0}"'.format(constants.ACL_URI_AUTH_USERS):
                        if 'id="' in who:
                            return expected_success_code

                    elif acl_v == 'uri="{0}"'.format(constants.ACL_URI_ALL_USERS):
                        return expected_success_code

        # if nothing gives grants - no grants
        return expected_bad_code
