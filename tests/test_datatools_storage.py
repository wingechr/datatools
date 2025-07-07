# coding: utf-8

import os
import unittest
from io import BytesIO
from tempfile import TemporaryDirectory

import pandas as pd

from datatools import Converter, Metadata, Storage


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
            storage.ressource("nonexistent/name").metadata.get("nonexistent_key"), None
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
        data = pd.DataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])

        @Converter.register(pd.DataFrame, ".csv")
        def df_to_csv(df: pd.DataFrame, encoding="utf-8", index=True):
            buf = BytesIO()
            # TODO: if index = True but has no names: generate names,
            # otherwise they will be renamed when loading (e.g. Unnamed: 0)
            df.to_csv(buf, encoding=encoding, index=index)
            buf.seek(0)
            return buf

        @Converter.register(".csv", pd.DataFrame)
        def csv_to_df(buf: BytesIO, encoding="utf-8", index_col=None):
            if isinstance(index_col, list):
                # if unnamed: replace with numerical index
                index_col = [c or i for i, c in enumerate(index_col)]

            df = pd.read_csv(buf, encoding=encoding, index_col=index_col)

            return df

        @Converter.register(pd.DataFrame, Metadata)
        def inspect_df(df: pd.DataFrame, encoding="utf-8"):
            return {
                "columns": df.columns.tolist(),
                # "dtypes": df.dtypes.to_dict(),
                "shape": df.shape,
                # index_col: important for loader
                "index_col": df.index.names,
            }

        # writing converter
        res.dump(data, metadata={}, encoding="utf-16")

        result = res.load()
        pd.testing.assert_frame_equal(data, result)
