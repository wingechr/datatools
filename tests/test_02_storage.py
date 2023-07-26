import logging
import unittest

from datatools.cache import DEFAULT_FROM_BYTES
from datatools.exceptions import DataDoesNotExists, DataExists, InvalidPath
from datatools.storage import DEFAULT_HASH_METHOD, HASHED_DATA_PATH_PREFIX, LocalStorage
from datatools.utils import exit_stack, normalize_path

from . import (
    MyTemporaryDirectory,
    b_hello,
    b_hello_world,
    md5_hello,
    md5_hello_world,
    objects_euqal,
)


class TestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = MyTemporaryDirectory()
        self.tempdir_path = exit_stack.enter_context(self.tempdir)
        self.storage = LocalStorage(location=self.tempdir_path)

    def tearDown(self) -> None:
        # make files writable so cleanup can delete them
        # for rt, _ds, fs in os.walk(self.tempdir_path):
        #    for f in fs:
        #        filepath = f"{rt}/{f}"
        #        make_file_writable(filepath)
        # self.tempdir.__exit__(None, None, None)
        pass


class Test_01_LocalStorage(TestBase):
    def test_storage(self):
        # create local instance in temporary dir

        data_path_user = "/My/path"
        invalid_path = HASHED_DATA_PATH_PREFIX + "my/path"

        logging.debug("Step 1: cannot save save data to hash subdir")
        self.assertRaises(
            InvalidPath,
            self.storage.data_put,
            data=b_hello_world,
            data_path=invalid_path,
        )

        logging.debug("Step 2a: save data without path")
        norm_data_path = self.storage.data_put(data=b_hello_world)
        self.assertTrue(norm_data_path.startswith(HASHED_DATA_PATH_PREFIX))
        self.assertEqual(
            norm_data_path,
            f"{HASHED_DATA_PATH_PREFIX}{DEFAULT_HASH_METHOD}/{md5_hello_world}",
        )

        logging.debug("Step 2b: save different data without path")
        norm_data_path = self.storage.data_put(data=b_hello)
        self.assertEqual(
            norm_data_path,
            f"{HASHED_DATA_PATH_PREFIX}{DEFAULT_HASH_METHOD}/{md5_hello}",
        )

        logging.debug("Step 2c: save with invalid path")
        self.assertRaises(
            InvalidPath,
            self.storage.data_put,
            data=b_hello_world,
            data_path="/my/data/file.metadata.json",  # .metadata. is not allowed
        )

        logging.debug("Step 2b: save data with path")
        self.assertFalse(self.storage.data_exists(data_path_user))
        data_path = self.storage.data_put(data=b_hello_world, data_path=data_path_user)
        self.assertTrue(self.storage.data_exists(data_path_user))
        self.assertEqual(normalize_path(data_path_user), data_path)

        logging.debug("Step 2c: save again will fail")
        self.assertRaises(
            DataExists,
            self.storage.data_put,
            data=b_hello_world,
            data_path=data_path_user,
        )

        logging.debug("Step 3: read data")
        # with self.storage.data_get(data_path=data_path_user) as file:
        #    res = file.read()
        res = self.storage.data_get(data_path=data_path_user).read()
        self.assertEqual(b_hello_world, res)

        logging.debug("Step 4a: delete")
        self.storage.data_delete(data_path=data_path_user)

        logging.debug("Step 4b: delete again")
        self.storage.data_delete(data_path=data_path_user)
        # reading now will raise error
        self.assertRaises(
            DataDoesNotExists, self.storage.data_get, data_path=data_path_user
        )

        logging.debug("Step 5: save again")
        self.storage.data_put(data=b_hello_world, data_path=data_path_user)

        logging.debug("Step 5: save metadata")
        metadata = {"a": [1, 2, 3], "b.c[0]": "test"}
        self.storage.metadata_put(data_path=data_path_user, metadata=metadata)

        logging.debug("Step 5: update metadata")
        metadata = {"b.c[1]": "test2"}
        self.storage.metadata_put(data_path=data_path_user, metadata=metadata)

        logging.debug("Step 5: get metadata")
        metadata2 = self.storage.metadata_get(
            data_path=data_path_user, metadata_path="b.c"
        )
        self.assertTrue(objects_euqal(metadata2, ["test", "test2"]))

    def test_storage_autoload(self):
        uri = "sqlite:///:memory:?q=select 1#/query1"
        self.assertRaises(DataDoesNotExists, self.storage.data_get, data_path=uri)
        data = self.storage.data_get(data_path=uri, auto_load_uri=True).read()
        df = DEFAULT_FROM_BYTES(data)
        self.assertEqual(df.iloc[0, 0], 1)

        uri = "sqlite:///:memory:?q=select 2#/query2"
        df2 = self.storage.data_get(data_path=uri, auto_load_uri=True, auto_decode=True)
        self.assertEqual(df2.iloc[0, 0], 2)
