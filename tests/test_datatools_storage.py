# coding: utf-8
import logging
import unittest
from io import BytesIO
from tempfile import TemporaryDirectory

from datatools import Storage

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.INFO
)


class TestTemplate(unittest.TestCase):
    def setUp(self):
        self.tempdir = TemporaryDirectory()

    def tearDown(self):
        self.tempdir.cleanup()

    def test_datatools_basics(self):
        storage = Storage(location=self.tempdir.name)
        res = storage.resource("a/b.txt")
        # read write metadata
        value = "myvalue"
        key = "mykey"
        res.metadata.set(key, value)
        self.assertEqual(res.metadata.get(key), value)

        # read write data
        self.assertFalse(res.exist())
        bdata = "test".encode()
        res.write(BytesIO(bdata))
        self.assertTrue(res.exist())
        with res.open() as file:
            self.assertTrue(file.read(), bdata)

        res.delete()
        self.assertFalse(res.exist())
