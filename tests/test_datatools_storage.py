# coding: utf-8

import os
import unittest
from io import BytesIO
from tempfile import TemporaryDirectory

import pandas as pd

from datatools import Converter, Storage


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
        res = storage.ressource("a/b.txt")

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

        res.delete()
        self.assertFalse(res.exist())
        self.assertFalse(res in storage.list())

        # create again without having a path
        res = storage.write(BytesIO(bdata), suffix=".txt")
        self.assertEqual(res.path, "md5/098f6bcd4621d373cade4e832627b4f6.txt")
        with res.open() as file:
            self.assertTrue(file.read(), bdata)

        # can get empty metadata without error, even if resource does not exist
        self.assertEqual(
            storage.ressource("nonexistent/path").metadata.get("nonexistent/key"), None
        )

    def test_datatools_storage_w_json_converter(self):
        storage = Storage(location=self.tempdir.name)
        res = storage.ressource("test.json")
        data = [1, 2, 3]

        res.dump(data)

        result = res.load()
        self.assertEqual(result, data)

    def test_datatools_storage_w_csv_df_converter(self):
        storage = Storage(location=self.tempdir.name)
        res = storage.ressource("test.csv")
        res.metadata.set(datatype=pd.DataFrame)
        data = pd.DataFrame([{"a": 1, "b": "2"}, {"a": 3, "b": "x"}]).set_index("a")

        @Converter.register(pd.DataFrame, ".csv")
        def df_to_csv(df: pd.DataFrame, **kwargs):
            buf = BytesIO()
            df.to_csv(buf, **kwargs)
            buf.seek(0)
            return buf

        @Converter.register(".csv", pd.DataFrame)
        def csv_to_df(buf: BytesIO, **kwargs):
            df = pd.read_csv(buf, **kwargs)
            return df

        writer_kwargs = {"encoding": "utf-16"}
        reader_kwargs = {"index_col": "a", "encoding": "utf-16"}
        res.dump(data, metadata=None, **writer_kwargs)

        result = res.load(**reader_kwargs)
        pd.testing.assert_frame_equal(data, result)

        # use resource as input
