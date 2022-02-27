from datatools.exceptions import DuplicateKeyException
from datatools.utils import (
    JsonSerializable,
    UniqueDict,
    detect_text_encoding_from_filepath,
    json_dumps,
)

from . import TestCase


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

    def test_chardet(self):
        for encoding, filename in [("utf-8", "mime.txt")]:
            filepath = self.get_data_filepath(filename)
            encoding_detected = detect_text_encoding_from_filepath(filepath)
            self.assertEqual(encoding, encoding_detected)
