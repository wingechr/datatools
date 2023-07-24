# coding: utf-8
import logging
import os
import unittest
from tempfile import TemporaryDirectory

from datatools.exceptions import DataDoesNotExists, DataExists, InvalidPath
from datatools.storage import HASHED_DATA_PATH_PREFIX, LocalStorage
from datatools.utils import make_file_writable, normalize_path

from . import objects_euqal

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
)


class Test_01_LocalStorage(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.tempdir_path = self.tempdir.__enter__()
        self.storage = LocalStorage(location=self.tempdir_path)

    def tearDown(self) -> None:
        # make files writable so cleanup can delete them
        for rt, _ds, fs in os.walk(self.tempdir_path):
            for f in fs:
                filepath = f"{rt}/{f}"
                make_file_writable(filepath)
        self.tempdir.__exit__(None, None, None)

    def test_storage(self):
        # create local instance in temporary dir
        data = b"hello world"
        data_path_user = "/My/path"
        invalid_path = HASHED_DATA_PATH_PREFIX + "my/path"

        # cannot save save data to hash subdir
        self.assertRaises(
            InvalidPath,
            self.storage.data_put,
            data=data,
            data_path=invalid_path,
        )
        # save data without path
        data_path = self.storage.data_put(data=data)
        self.assertTrue(data_path.startswith(HASHED_DATA_PATH_PREFIX))

        # save data
        data_path = self.storage.data_put(data=data, data_path=data_path_user)
        self.assertEqual(normalize_path(data_path_user), data_path)
        # save again will fail
        self.assertRaises(
            DataExists,
            self.storage.data_put,
            data=data,
            data_path=data_path_user,
        )
        # read it
        res = self.storage.data_get(data_path=data_path_user)
        self.assertEqual(data, res)
        # delete it ...
        self.storage.data_delete(data_path=data_path_user)
        # ... and deleting again does NOT raise an error ...
        self.storage.data_delete(data_path=data_path_user)
        # reading now will raise error
        self.assertRaises(
            DataDoesNotExists, self.storage.data_get, data_path=data_path_user
        )
        # ... and now we can save it again
        self.storage.data_put(data=data, data_path=data_path_user)

        # metadata can be saved independent of data
        metadata = {"a": [1, 2, 3], "b.c[0]": "test"}
        self.storage.metadata_put(data_path=data_path_user, metadata=metadata)

        # partial update
        metadata = {"b.c[1]": "test2"}
        self.storage.metadata_put(data_path=data_path_user, metadata=metadata)

        # get partial
        metadata2 = self.storage.metadata_get(
            data_path=data_path_user, metadata_path="b.c"
        )
        self.assertTrue(objects_euqal(metadata2, ["test", "test2"]))
