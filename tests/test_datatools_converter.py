# coding: utf-8
import logging
import unittest
from tempfile import TemporaryDirectory

from datatools.converter import get_converter, register_converter

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.INFO
)


class TestDatatoolsConverter(unittest.TestCase):
    def setUp(self):
        self.tempdir = TemporaryDirectory()

    def tearDown(self):
        self.tempdir.cleanup()

    def test_datatools_converter_basics(self):

        @register_converter(str, int)
        def str_to_int(s: str) -> int:
            return int(s)

        # decorated function still works
        self.assertEqual(str_to_int("1"), 1)

        # decorated function can be found with get_converter
        self.assertEqual(get_converter(str, int)("1"), 1)
