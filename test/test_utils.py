import logging

from . import TestCase

from datatools.utils import (
    UniqueDict,
    JsonSerializable,
    json_dumps,
    detect_mime_from_filepath,
    detect_mime_from_filepath_bytes,
    detect_text_encoding_from_filepath,
)
from datatools.exceptions import DuplicateKeyException


class TestUniqueDict(TestCase):
    def test_unique(self):
        d = UniqueDict([("a", 1), ("b", 2)])
        d["c"] = 3

        self.assertEqual(d["c"], 3)

        def add_a_again():
            d["a"] = 1

        self.assertRaises(DuplicateKeyException, add_a_again)


class TestJsonSerializable(TestCase):
    def test_unique(self):
        class J(JsonSerializable):
            def __init__(self, a):
                self.a = a

        j = J(1)
        self.assertEqual(json_dumps(j), json_dumps({"a": 1}))

        # recursieve
        j = J(J(1))
        self.assertEqual(json_dumps(j), json_dumps({"a": {"a": 1}}))


class TestMime(TestCase):
    def test_mime(self):
        for mime_file, mime_bytes, filename in [
            ("text/plain", "text/plain", "mime.txt"),
            (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "mime.xlsx",
            ),
            ("text/plain", "text/plain", "mime.json"),  # 'application/json',
        ]:
            filepath = self.get_data_filepath(filename)
            mime_file_detected = detect_mime_from_filepath(filepath)
            mime_bytes_detected = detect_mime_from_filepath_bytes(filepath)

            self.assertEqual(mime_file, mime_file_detected)
            self.assertEqual(mime_bytes, mime_bytes_detected)

    def test_chardet(self):
        for encoding, filename in [("utf-8", "mime.txt")]:
            filepath = self.get_data_filepath(filename)
            encoding_detected = detect_text_encoding_from_filepath(filepath)
            self.assertEqual(encoding, encoding_detected)
