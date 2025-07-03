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

        # create resource
        res = storage.get("a/b.txt")

        # read/write metadata
        key = "$mykey"
        value = 999

        res.metadata.set(**{key: value})
        self.assertEqual(res.metadata.get(key), value)

        # read write data
        # data does not exist, even though we saved metadata
        self.assertFalse(res.exist())

        bdata = "test".encode()
        res.write(BytesIO(bdata))
        self.assertTrue(res.exist())
        with res.open() as file:
            self.assertTrue(file.read(), bdata)

        # test list all resources and equality
        self.assertTrue(res in storage.list())

        # delete (but keep metadata)
        res.delete(keep_metadata=True)
        self.assertFalse(res.exist())
        # -> still in storage, because metadata still exists
        self.assertTrue(res in storage.list())
        res.delete()
        self.assertFalse(res in storage.list())

        # create again without having a path
        res = storage.store(BytesIO(bdata), suffix=".txt")
        self.assertEqual(res.path, "md5/098f6bcd4621d373cade4e832627b4f6.txt")
        with res.open() as file:
            self.assertTrue(file.read(), bdata)

        # can get empty metadata without error, even if resource does not exist
        self.assertEqual(
            storage.get("nonexistent/path").metadata.get("nonexistent/key"), None
        )
