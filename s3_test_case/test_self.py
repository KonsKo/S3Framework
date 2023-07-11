"""Test case for custom method assertDictContains."""
import json
import random

from helpers.utils import BucketPolicy, DictDuplicateKey
from s3_test_case.test_case import DictContains, S3AsyncTestCase


def random_additional_keys(arg):
    random.seed(str(arg))
    rslt = dict(arg)
    random_keys = ['first', 'third', 'fourth', 'another', 1, 2]
    random_items = [None, 1, 2, 'a', 'b', {}, [1]]
    for _ in range(random.randint(0, 3)):
        rslt[random.choice(random_keys)] = random.choice(random_items)
    return rslt


class TestDictContains(S3AsyncTestCase):

    s3_session = False

    # Simple values
    def test_none_ok(self):
        self.assertDictContains({'first': None, 'second': 2}, first=None)

        with self.assertRaises(self.failureException):
            self.assertDictContains({'first': 1, 'second': 2}, first=None)

    def test_value(self):
        self.assertDictContains({'first': 1, 'second': 2}, first=1)

        with self.assertRaises(self.failureException):
            self.assertDictContains({'first': None, 'second': 2}, first=1)

        with self.assertRaises(self.failureException):
            self.assertDictContains({'first': [], 'second': 2}, first=1)

    def test_values2_ok(self):
        self.assertDictContains({'first': 1, 'second': 2, 'third': {}},
                                first=1, second=2)

        with self.assertRaises(self.failureException):
            self.assertDictContains({'first': 1, 'second': {}, 'third': 2},
                                    first=1, second=2)

    def test_field_existed(self):
        """Test field is presented in dict, no value checked."""
        self.assertDictContains(
            {'first': 1, 'second': 2},
            first=self.InDict(),
        )

        self.assertDictContains(
            {'first': None, 'second': 2},
            first=self.InDict(),
        )

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                {'first': 1, 'second': 2},
                not_existed=self.InDict(),
            )

    # Strict dict matching
    def test_nested_dict_ok(self):
        self.assertDictContains({'first': 1, 'second': {'a': 1, 'b': 2}, 'third': 3},
                                second={'a': 1, 'b': 2})

        with self.assertRaises(self.failureException):
            self.assertDictContains({'first': 1, 'second': {'a': 1, 'b': 2}, 'third': 3},
                                    second={'a': 1, 'b': 3})

        with self.assertRaises(self.failureException):
            self.assertDictContains({'first': 1, 'second': {'a': 1, 'b': 2}, 'third': 3},
                                    second={'a': 1})

    # Simple list values
    def test_list_basic(self):
        self.assertDictContains({'first': 1, 'second': ['a'], 'third': 3},
                                second=['a'])

        with self.assertRaises(self.failureException):
            self.assertDictContains({'first': 1, 'second': ['a'], 'third': 3},
                                    second=[None])

        with self.assertRaises(self.failureException):
            self.assertDictContains({'first': 1, 'second': ['a', 'b'], 'third': 3},
                                    second=['a'])

    def test_list2_ok(self):
        self.assertDictContains({'first': 1, 'second': ['a', 'b'], 'third': 3},
                                second=['a', 'b'])

        self.assertDictContains({'first': 1, 'second': ['a', 'b'], 'third': 3},
                                second=['b', 'a'])

    def test_list2_dup(self):
        self.assertDictContains({'first': 1, 'second': ['a', 'a'], 'third': 3},
                                second=['a', 'a'])

        with self.assertRaises(self.failureException):
            self.assertDictContains({'first': 1, 'second': ['a', 'b'], 'third': 3},
                                    second=['a', 'a'])

        with self.assertRaises(self.failureException):
            self.assertDictContains({'first': 1, 'second': ['b', 'a'], 'third': 3},
                                    second=['a', 'a'])

    # Complex nested list of dicts
    def test_list_dict_equal(self):
        self.assertDictContains(
            random_additional_keys({
                'second': [{'nested1': 'a', 'nested2': 'b'}],
            }),
            second=[{'nested1': 'a', 'nested2': 'b'}])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [{'nested1': 'a', 'nested2': 'b'}],
                    'third': 3,
                }),
                second=[{'nested1': 'a'}])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [{'nested1': 'a', 'nested2': 'b'}],
                }),
                second=[{'nested1': 'a', 'nested2': 'wrong'}])

    def test_list_dict_contains_partial(self):
        self.assertDictContains(
            random_additional_keys({
                'second': [{'nested1': 'a', 'nested2': 'b'}],
            }),
            second=[DictContains()],
        )

        self.assertDictContains(
            random_additional_keys({
                'second': [{'nested1': 'a', 'nested2': 'b'}],
            }),
            second=[DictContains(nested1='a')])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [{'nested1': 'a', 'nested2': 'b'}],
                }),
                second=[DictContains(nested1='b')])

    def test_list_dict2_equal(self):
        self.assertDictContains(
            random_additional_keys({
                'second': [{'nested1': 'a', 'nested2': 'b'}],
            }),
            second=[{'nested1': 'a', 'nested2': 'b'}])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [{'nested1': 'a', 'nested2': 'b'}],
                }),
                second=[{'nested1': 'a', 'nested2': 'wrong'}])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [{'nested1': 'a', 'nested2': 'b'}, {}],
                }),
                second=[{'nested1': 'a'}])

    def test_list_dict2_partial(self):
        self.assertDictContains(
            random_additional_keys({
                'second': [{'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'}],
            }),
            second=[DictContains(nested1='a', nested2='b')])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [{'nested1': 'a', 'nested2': 'b', 'nested3': 'c'}],
                }),
                second=[DictContains(nested1='a', nested2='wrong')])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [{'nested1': 'a', 'nested2': 'b'}, {}],
                }),
                second=[DictContains(nested1='a')])

    def test_list2_dict2_empty(self):
        self.assertDictContains(
            random_additional_keys({
                'second': [
                    {'nested1': 'a', 'nested2': 'b'},
                    {},
                ],
            }),
            second=[
                {'nested1': 'a', 'nested2': 'b'},
                {},
            ])

        self.assertDictContains(
            random_additional_keys({
                'second': [
                    {'nested1': 'a', 'nested2': 'b'},
                    {},
                ],
            }),
            second=[
                {},
                {'nested1': 'a', 'nested2': 'b'},
            ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b'},
                        {},
                    ],
                }),
                second=[
                    {'nested1': 'a', 'nested2': 'wrong'},
                    {},
                ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b', 'nested3': 'c'},
                        {},
                    ],
                }),
                second=[
                    {},
                    {'nested1': 'a', 'nested2': 'b'},
                ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b'},
                        {},
                    ],
                }),
                second=[
                    {'nested1': 'a'},
                    {'nested1': 'a', 'nested2': 'b'},
                ])

    def test_list2_dict2_empty_partial(self):
        self.assertDictContains(
            random_additional_keys({
                'second': [
                    {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                    {'nested2': 'e', 'nested3': 'f'},
                ],
            }),
            second=[
                DictContains(nested1='a', nested2='b'),
                DictContains(),
            ])

        self.assertDictContains(
            random_additional_keys({
                'second': [
                    {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                    {'nested2': 'e', 'nested3': 'f'},
                ],
            }),
            second=[
                DictContains(),
                DictContains(nested1='a', nested2='b'),
            ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                        {'nested2': 'e', 'nested3': 'f'},
                    ],
                }),
                second=[
                    DictContains(nested1='a', nested2='wrong'),
                    DictContains(),
                ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                        {'nested2': 'e', 'nested3': 'f'},
                    ],
                }),
                second=[
                    DictContains(),
                    DictContains(nested1='a', nested2='wrong'),
                ])

    def test_list2_dict2_mixed(self):
        self.assertDictContains(
            random_additional_keys({
                'second': [
                    {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                    {'nested2': 'e', 'nested3': 'f'},
                ],
            }),
            second=[
                {'nested2': 'e', 'nested3': 'f'},
                DictContains(nested1='a', nested2='b'),
            ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                        {'nested2': 'e', 'nested3': 'f'},
                    ],
                }),
                second=[
                    {'nested1': 'wrong'},
                    DictContains(nested1='a', nested2='b'),
                ])

        # Ensure scoring is working correctly and DictContains() matches the remaining item only
        self.assertDictContains(
            random_additional_keys({
                'second': [
                    {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                    {'nested2': 'e', 'nested3': 'f'},
                ],
            }),
            second=[
                {'nested2': 'e', 'nested3': 'f'},
                DictContains(),
            ])

        self.assertDictContains(
            random_additional_keys({
                'second': [
                    {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                    {'nested2': 'e', 'nested3': 'f'},
                ],
            }),
            second=[
                DictContains(),
                {'nested2': 'e', 'nested3': 'f'},
            ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                        {'nested2': 'e', 'nested3': 'f'},
                    ],
                }),
                second=[
                    {'nested1': 'a', 'nested2': 'b'},
                    DictContains(nested2='e', nested3='f'),
                ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                        {'nested2': 'e', 'nested3': 'f'},
                    ],
                }),
                second=[
                    # same item
                    {'nested2': 'e', 'nested3': 'f'},
                    DictContains(nested2='e', nested3='f'),
                ])

    def test_list2_dict2_duplicate(self):
        self.assertDictContains(
            random_additional_keys({
                'second': [
                    {'nested1': 'a', 'nested2': 'b'},
                    {'nested1': 'a', 'nested2': 'b'},
                    {'nested2': 'e', 'nested3': 'f'},
                    {'nested2': 'e', 'nested3': 'f'},
                ],
            }),
            second=[
                {'nested1': 'a', 'nested2': 'b'},
                {'nested1': 'a', 'nested2': 'b'},
                {'nested2': 'e', 'nested3': 'f'},
                {'nested2': 'e', 'nested3': 'f'},
            ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b'},
                        {'nested1': 'a', 'nested2': 'b'},
                        {'nested2': 'e', 'nested3': 'f'},
                        {'nested2': 'e', 'nested3': 'f'},
                    ],
                }),
                second=[
                    {'nested1': 'a', 'nested2': 'b'},
                    {'nested1': 'a', 'nested2': 'b'},
                    {'nested2': 'e', 'nested3': 'f'},
                ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b'},
                        {'nested1': 'a', 'nested2': 'b'},
                        {'nested2': 'e', 'nested3': 'f'},
                        {'nested2': 'e', 'nested3': 'f'},
                    ],
                }),
                second=[
                    {'nested1': 'a', 'nested2': 'b'},
                    {'nested1': 'a', 'nested2': 'b'},
                    {'nested2': 'e', 'nested3': 'f'},
                    {},
                ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b'},
                        {'nested2': 'e', 'nested3': 'f'},
                    ],
                }),
                second=[
                    {},
                    {'nested1': 'a', 'nested2': 'b'},
                    {'nested1': 'a', 'nested2': 'b'},
                ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b'},
                        {'nested2': 'e', 'nested3': 'f'},
                    ],
                }),
                second=[
                    {},
                    {'nested1': 'a', 'nested2': 'b'},
                    {'nested2': 'e', 'nested3': 'wrong'},
                ])

    def test_list2_dict2_duplicate_partial(self):
        self.assertDictContains(
            random_additional_keys({
                'second': [
                    {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                    {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong2'},
                    {'nested2': 'e', 'nested3': 'f'},
                ],
            }),
            second=[
                DictContains(nested1='a', nested2='b'),
                DictContains(nested1='a', nested2='b'),
                DictContains(nested2='e', nested3='f'),
            ])

        self.assertDictContains(
            random_additional_keys({
                'second': [
                    {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                    {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong2'},
                    {'nested2': 'e', 'nested3': 'f'},
                ],
            }),
            second=[
                DictContains(nested1='a', nested2='b'),
                DictContains(nested1='a', nested2='b'),
                DictContains(),
            ])

        self.assertDictContains(
            random_additional_keys({
                'second': [
                    {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                    {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong2'},
                    {'nested2': 'e', 'nested3': 'f'},
                ],
            }),
            second=[
                DictContains(),
                DictContains(nested1='a', nested2='b'),
                DictContains(nested1='a', nested2='b'),
            ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                        {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong2'},
                        {'nested2': 'e', 'nested3': 'f'},
                    ],
                }),
                second=[
                    DictContains(),
                    DictContains(nested1='a', nested2='b'),
                ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                        {'nested2': 'e', 'nested3': 'f'},
                        {'nested2': 'e', 'nested3': 'f'},
                    ],
                }),
                second=[
                    DictContains(),
                    DictContains(nested1='a', nested2='b'),
                    DictContains(nested1='a', nested2='b'),
                ])

        with self.assertRaises(self.failureException):
            self.assertDictContains(
                random_additional_keys({
                    'second': [
                        {'nested1': 'a', 'nested2': 'b', 'nested3': 'wrong'},
                        {'nested2': 'e', 'nested3': 'f'},
                    ],
                }),
                second=[
                    DictContains(),
                    DictContains(nested1='a', nested2='b'),
                    DictContains(nested1='a', nested2='b'),
                ])


class TestCaseDictKeysDuplicates(S3AsyncTestCase):

    s3_session = False

    def test_duplicate_keys_in_condition(self):
        """Test policy creation with duplicate keys."""
        duplicated_key = 'key_d'
        duplicated_key_mod = '{0}_'.format(duplicated_key)

        condition = DictDuplicateKey([(duplicated_key, '1'), (duplicated_key, 2)])
        condition_mod = {duplicated_key_mod: 2, duplicated_key: '1'}

        bp = BucketPolicy(
            statement=[
                {
                    'sid': 'id-1',
                    'principal': '123',
                    'action': '*',
                    'resource': '*',
                    'effect': 'Allow',
                    'condition': condition,
                },
            ],
        )
        bp_mod = BucketPolicy(
            statement=[
                {
                    'sid': 'id-1',
                    'principal': '123',
                    'action': '*',
                    'resource': '*',
                    'effect': 'Allow',
                    'condition': condition_mod,
                },
            ],
        )
        policy_serialized_mod = bp_mod.as_string_json()

        policy_serialized = bp.as_string_json()

        # change one of duplicate keys to `duplicated_key_mod` to allow make deserialization
        # and comparison
        policy_serialized = policy_serialized.replace(
            duplicated_key, '{0}_'.format(duplicated_key), 1,
        )

        policy_deserialized = json.loads(policy_serialized)
        policy_deserialized_mod = json.loads(policy_serialized_mod)

        self.assertEqual(
            policy_deserialized,
            policy_deserialized_mod,
        )


class TestCaseBucketPolicyCreation(S3AsyncTestCase):

    s3_session = False

    def test_create_bucket_policy_check_fields_regular(self):
        bp = BucketPolicy(
            statement=[
                {
                    'sid': 'id-1',
                    'effect': 'Allow',
                    'principal': '1234567890',
                    'action': ['PutObject', 'GetObject'],
                    'resource': 'bucket1',
                    'condition': {'cond_key': 'cond_val'},
                },
            ],
        )
        self.assertDictContains(
            bp.as_dict().get('Statement')[0],
            Sid=self.InDict(),
            Effect=self.InDict(),
            Principal=self.InDict(),
            NotPrincipal=None,
            Action=self.InDict(),
            NotAction=None,
            Resource=self.InDict(),
            NotResource=None,
            Condition=self.InDict(),
        )

    def test_create_bucket_policy_check_fields_with_negative(self):
        bp = BucketPolicy(
            statement=[
                {
                    'sid': 'id-1',
                    'effect': 'Allow',
                    'principal': '1234567890',
                    'not_principal': '1234567890',
                    'action': ['PutObject', 'GetObject'],
                    'not_action': ['PutObject', 'GetObject'],
                    'resource': 'bucket1',
                    'not_resource': 'bucket1',
                },
            ],
        )
        self.assertDictContains(
            bp.as_dict().get('Statement')[0],
            Sid=self.InDict(),
            Effect=self.InDict(),
            Principal=self.InDict(),
            NotPrincipal=self.InDict(),
            Action=self.InDict(),
            NotAction=self.InDict(),
            Resource=self.InDict(),
            NotResource=self.InDict(),
            Condition=None,
        )

    def test_create_bucket_policy_check_fields_only_negative(self):
        bp = BucketPolicy(
            statement=[
                {
                    'sid': 'id-1',
                    'effect': 'Allow',
                    'not_principal': '1234567890',
                    'not_action': ['PutObject', 'GetObject'],
                    'not_resource': 'bucket1',
                    'condition': None,
                },
            ],
        )
        self.assertDictContains(
            bp.as_dict().get('Statement')[0],
            Sid=self.InDict(),
            Effect=self.InDict(),
            Principal=None,
            NotPrincipal=self.InDict(),
            Action=None,
            NotAction=self.InDict(),
            Resource=None,
            NotResource=self.InDict(),
            Condition=None,
        )

    def test_create_bucket_policy_check_fields_absence(self):
        """
        Check bucket policy processing with explicit absence of fields.

        Test subject:
            self

        Test description:
            Check bucket policy processing with explicit absence of fields.

        Checked with AWS:
            False
        """
        bp = BucketPolicy(
            statement=[
                {
                },
            ],
        )
        self.assertDictContains(
            bp.as_dict().get('Statement')[0],
            Sid=None,
            Effect=None,
            Principal=None,
            NotPrincipal=None,
            Action=None,
            NotAction=None,
            Resource=None,
            NotResource=None,
            Condition=None,
        )

    def test_create_bucket_policy_check_fields_multiple_statement(self):
        """
        Check bucket policy processing with explicit multiple values of fields.

        Test subject:
            self

        Test description:
            Check bucket policy processing with explicit multiple values of fields.

        Checked with AWS:
            False
        """
        bp = BucketPolicy(
            statement=[
                {
                    'sid': 'id-1',
                    'effect': 'Allow',
                    'principal': '1234567890',
                    'action': ['PutObject', 'GetObject'],
                    'resource': 'bucket1',
                },
                {
                    'sid': 'id-1',
                    'effect': 'Allow',
                    'not_principal': '1234567890',
                    'not_action': ['PutObject', 'GetObject'],
                    'not_resource': 'bucket1',
                },
                {
                },
            ],
        )
        self.assertDictContains(
            bp.as_dict().get('Statement')[0],
            Sid=self.InDict(),
            Effect=self.InDict(),
            Principal=self.InDict(),
            NotPrincipal=None,
            Action=self.InDict(),
            NotAction=None,
            Resource=self.InDict(),
            NotResource=None,
            Condition=None,
        )
        self.assertDictContains(
            bp.as_dict().get('Statement')[1],
            Sid=self.InDict(),
            Effect=self.InDict(),
            Principal=None,
            NotPrincipal=self.InDict(),
            Action=None,
            NotAction=self.InDict(),
            Resource=None,
            NotResource=self.InDict(),
            Condition=None,
        )
        self.assertDictContains(
            bp.as_dict().get('Statement')[2],
            Sid=None,
            Effect=None,
            Principal=None,
            NotPrincipal=None,
            Action=None,
            NotAction=None,
            Resource=None,
            NotResource=None,
            Condition=None,
        )
