# coding: utf-8

import unittest
from typing import Callable

from datatools.converter import Converter, UriHandlerType


class TestDatatoolsConverter(unittest.TestCase):
    def test_datatools_converter_basics(self):

        @Converter.register(str, int)
        def str_to_int(s: str) -> int:
            return int(s)

        # decorated function still works
        self.assertEqual(str_to_int("1"), 1)

        # decorated function can be found with get_converter
        self.assertEqual(Converter.get(str, int)("1"), 1)

    def test_datatools_converter_handlers(self):
        # url = "http://example.com"
        scheme = "http:"
        handler = Converter.get(scheme, UriHandlerType)
        self.assertTrue(isinstance(handler, Callable))  # type: ignore

    def test_datatools_converter_keep_signature(self):
        def fun(a: str) -> float:
            return float(a)

        fun2 = Converter.autoregister(fun)

        # check if it also works after we decorate it as Converter
        self.assertEqual(fun.__annotations__, fun2.__annotations__)
