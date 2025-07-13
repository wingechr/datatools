# coding: utf-8

import os
import unittest
from io import BytesIO
from tempfile import TemporaryDirectory

import pandas as pd

from datatools import Storage


class TestDatatoolsStorage(unittest.TestCase):
    def setUp(self):
        self.tempdir = TemporaryDirectory()
        assert os.path.exists(self.tempdir.name)

    def tearDown(self):
        self.tempdir.cleanup()
        assert not os.path.exists(self.tempdir.name)

    def test_datatools_storage_basics(self):
        storage = Storage(location=self.tempdir.name)

        # create resource
        res = storage.resource("a/b.txt")

        # read/write metadata
        key = "mykey"
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

        res.delete()
        self.assertFalse(res.exist())
        self.assertFalse(res in storage.list())

        # create again without having a path
        res = storage.write(BytesIO(bdata), suffix=".txt")
        self.assertEqual(res.name, "md5/098f6bcd4621d373cade4e832627b4f6.txt")
        with res.open() as file:
            self.assertTrue(file.read(), bdata)

        # can get empty metadata without error, even if resource does not exist
        self.assertEqual(
            storage.resource("nonexistent/name").metadata.get("nonexistent_key"), None
        )

    def test_datatools_storage_w_json_converter(self):
        storage = Storage(location=self.tempdir.name)
        res = storage.resource("test.json")
        data = [1, 2, 3]

        res.dump(data)
        result = res.load()
        self.assertEqual(result, data)

    def test_datatools_storage_w_csv_df_converter(self):
        storage = Storage(location=self.tempdir.name)
        res = storage.resource("test.csv")
        res.metadata.set(datatype=pd.DataFrame)
        data = pd.DataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])

        # writing converter
        res.dump(data, metadata={}, encoding="utf-16")

        # test that metadata should also include encoding
        self.assertEqual(res.metadata.get("encoding"), "utf-16")

        df = res.load()

        pd.testing.assert_frame_equal(data, df)
