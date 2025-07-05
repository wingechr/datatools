# coding: utf-8

import unittest
from typing import Union

from datatools.utils import (
    get_keyword_only_parameters_types,
    jsonpath_get,
    jsonpath_update,
)


class TestDatatoolsUtils(unittest.TestCase):
    def test_datatools_utils_jsonpath(self):
        data = {
            "key1": "value1",
            "key2": [
                {"key3": "value3", "key4": "value4"},
                {"key3": "value5", "key4": "value6"},
            ],
        }

        self.assertEqual(jsonpath_get(data, "key1"), "value1")
        self.assertEqual(jsonpath_get(data, "$.key1"), "value1")
        self.assertEqual(jsonpath_get(data, "'key1'"), "value1")
        self.assertEqual(jsonpath_get(data, "key2[0].key3"), "value3")
        self.assertEqual(jsonpath_get(data, "key2[*].key3"), ["value3", "value5"])

        jsonpath_update(data, "key4", "value7")
        self.assertEqual(jsonpath_get(data, "key4"), "value7")

    def test_datatools_utils_get_parameters(self):
        def fun(
            a1,  # kind=POSITIONAL_OR_KEYWORD
            a2,  # kind=POSITIONAL_OR_KEYWORD
            *args: str,  # kind=VAR_POSITIONAL
            k1: Union[int, None] = None,  # kind=KEYWORD_ONLY
            k2: Union[str, None] = None,  # kind=KEYWORD_ONLY
            **kwargs: dict,  # kind=VAR_KEYWORD
        ):
            pass

        self.assertEqual(get_keyword_only_parameters_types(fun, min_idx=4), ["k2"])
