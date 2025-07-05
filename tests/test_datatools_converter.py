# coding: utf-8

import unittest
from tempfile import TemporaryDirectory

from datatools import Converter


class TestDatatoolsConverter(unittest.TestCase):
    def setUp(self):
        self.tempdir = TemporaryDirectory()

    def tearDown(self):
        self.tempdir.cleanup()

    def test_datatools_converter_basics(self):

        @Converter.register(str, int)
        def str_to_int(s: str) -> int:
            return int(s)

        # decorated function still works
        self.assertEqual(str_to_int("1"), 1)

        # decorated function can be found with get_converter
        self.assertEqual(Converter.get(str, int)("1"), 1)
