import unittest
import logging

from datatools.utils import UniqueDict, JsonSerializable, json_dumps
from datatools.exceptions import DuplicateKeyException


class TestUniqueDict(unittest.TestCase):
    def test_unique(self):
        d = UniqueDict([("a", 1), ("b", 2)])
        d["c"] = 3

        self.assertEqual(d["c"], 3)

        def add_a_again():
            d["a"] = 1

        self.assertRaises(DuplicateKeyException, add_a_again)


class TestJsonSerializable(unittest.TestCase):
    def test_unique(self):
        class J(JsonSerializable):
            def __init__(self, a):
                self.a = a

        j = J(1)
        self.assertEqual(json_dumps(j), json_dumps({"a": 1}))

        # recursieve
        j = J(J(1))
        self.assertEqual(json_dumps(j), json_dumps({"a": {"a": 1}}))
