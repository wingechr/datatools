# coding: utf-8

import unittest
from typing import Callable, Union

from datatools import Converter
from datatools.utils import (
    filepath_from_uri,
    get_function_parameters_datatypes,
    get_keyword_only_parameters_types,
    get_resource_path_name,
    get_suffix,
    is_type_class,
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

    def test_get_parameters_types(self):
        def fun(a: str) -> float:
            return float(a)

        self.assertEqual(get_function_parameters_datatypes(fun), {"a": str})

        # check if it also works after we decorate it as Converter
        fun = Converter.autoregister(fun)

        self.assertEqual(get_function_parameters_datatypes(fun), {"a": str})

    def test_is_type(self):
        self.assertTrue(is_type_class(int))
        self.assertFalse(is_type_class("test"))
        self.assertFalse(is_type_class(None))
        self.assertTrue(is_type_class(Callable))
        self.assertTrue(is_type_class(Union[str, None]))

    def test_datatools_utils_get_resource_path_name(self):
        for name, valid_name in [("A", "a")]:
            self.assertEqual(get_resource_path_name(name), valid_name)

    def test_datatools_utils_filepath_from_uri(self):
        self.assertEqual(filepath_from_uri("file:///c:/path").as_posix(), "c:/path")
        self.assertEqual(filepath_from_uri("file://host/c:/path").as_posix(), "c:/path")
        self.assertEqual(filepath_from_uri("file:///path").as_posix(), "/path")
        self.assertEqual(filepath_from_uri("file://host/path").as_posix(), "/path")

    def test_get_suffix(self):
        self.assertEqual(get_suffix("file.suffix"), ".suffix")
        self.assertEqual(get_suffix("path.path/file.suffix"), ".suffix")
        self.assertEqual(get_suffix("path.path/file.x.suffix"), ".x.suffix")
        self.assertEqual(get_suffix("path.path#file.suffix"), ".suffix")
