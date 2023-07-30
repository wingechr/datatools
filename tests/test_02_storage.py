import logging
import os
import subprocess as sp
import sys

# import secrets
import unittest
from tempfile import TemporaryDirectory

from datatools.cache import DEFAULT_FROM_BYTES
from datatools.exceptions import DataDoesNotExists, DataExists, InvalidPath
from datatools.storage import HASHED_DATA_PATH_PREFIX, Storage
from datatools.utils import (
    DEFAULT_BUFFER_SIZE,
    get_free_port,
    make_file_writable,
    normalize_path,
    platform_is_unix,
    wait_for_server,
)

from . import objects_euqal


class MyTemporaryDirectory(TemporaryDirectory):
    def __init__(self, *args, **kwargs):
        self.tempdir = TemporaryDirectory(*args, **kwargs)

    def __enter__(self):
        return self.tempdir.__enter__()

    def __exit__(self, *args):
        # make files writable so cleanup can delete them
        path = self.tempdir.name
        for rt, _ds, fs in os.walk(path):
            for f in fs:
                filepath = f"{rt}/{f}"
                make_file_writable(filepath)
        # delete dir
        self.tempdir.__exit__(*args)
        logging.debug(f"DELETING {path}: {not os.path.exists(path)}")


class TestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir1 = MyTemporaryDirectory()
        path_tempdir1 = self.tempdir1.__enter__()  # exit_stack.enter_context()
        self.storage = Storage(location=path_tempdir1)

        # set up static file server
        self.tempdir2 = MyTemporaryDirectory()
        self.static_dir = self.tempdir2.__enter__()  # exit_stack.enter_context()
        port = get_free_port()
        self.static_url = f"http://localhost:{port}"
        self.http_proc = sp.Popen(
            [
                sys.executable,
                "-m",
                "http.server",
                str(port),
                "--directory",
                self.static_dir,
            ],
            stdout=sp.DEVNULL,  # do not show server startup message
            stderr=sp.DEVNULL,  # do not show server request logging
        )
        wait_for_server(self.static_url)

    def tearDown(self) -> None:
        self.http_proc.terminate()  # or kill
        self.http_proc.wait()

        self.tempdir1.__exit__(None, None, None)
        self.tempdir2.__exit__(None, None, None)


class Test_01_LocalStorage(TestBase):
    def test_storage(self):
        # create local instance in temporary dir

        # create large random data
        # data = secrets.token_bytes(int(DEFAULT_BUFFER_SIZE * 1.5))
        data = b"x" * int(DEFAULT_BUFFER_SIZE * 1.5)

        data_path_user = "/My/path"
        invalid_path = HASHED_DATA_PATH_PREFIX + "my/path"

        logging.debug("Step 1: cannot save save data to hash subdir")
        self.assertRaises(
            InvalidPath,
            self.storage.data_put,
            data=data,
            data_path=invalid_path,
        )

        logging.debug("Step 2a: save data without path")
        data_path = self.storage.data_put(data=data)
        self.assertTrue(data_path.startswith(HASHED_DATA_PATH_PREFIX))

        # load this again
        with self.storage.data_open(data_path) as file:
            _data = file.read()
        self.assertEqual(_data, data)

        logging.debug("Step 2c: save with invalid path")
        self.assertRaises(
            InvalidPath,
            self.storage.data_put,
            data=data,
            data_path="/my/data/file.metadata.json",  # .metadata. is not allowed
        )

        logging.debug("Step 2b: save data with path")
        self.assertFalse(self.storage.data_exists(data_path_user))
        data_path = self.storage.data_put(data=data, data_path=data_path_user)
        self.assertTrue(self.storage.data_exists(data_path_user))
        self.assertEqual(normalize_path(data_path_user), data_path)

        logging.debug("Step 2c: save again will fail")
        self.assertRaises(
            DataExists,
            self.storage.data_put,
            data=data,
            data_path=data_path_user,
        )

        logging.debug("Step 3: read data")
        with self.storage.data_open(data_path=data_path_user) as file:
            _data = file.read()
        self.assertEqual(data, _data)

        logging.debug("Step 4a: delete")
        self.storage.data_delete(data_path=data_path_user)

        logging.debug("Step 4b: delete again")
        self.storage.data_delete(data_path=data_path_user)
        # reading now will raise error
        self.assertRaises(
            DataDoesNotExists, self.storage.data_open, data_path=data_path_user
        )

        logging.debug("Step 5: save again")
        self.storage.data_put(data=data, data_path=data_path_user)

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
        uri = "sqlite:///:memory:?q=select 1 as value#/query1"
        self.assertRaises(DataDoesNotExists, self.storage.data_open, data_path=uri)

        with self.storage.data_open(data_path=uri, auto_load_uri=True) as file:
            data = file.read()
        data = DEFAULT_FROM_BYTES(data)
        self.assertEqual(data[0]["value"], 1)

        # save test file
        db_filepath = self.static_dir + "/test.db"
        # file should be created by sqlalchemy

        # only for sqlite:
        # in need an additional slash in linux for abs path
        if platform_is_unix:
            db_filepath = "/" + db_filepath

        uri = f"sqlite://{db_filepath}?q=select 1 as value#/query1"
        with self.storage.data_open(data_path=uri, auto_load_uri=True) as file:
            data = file.read()
        data = DEFAULT_FROM_BYTES(data)
        self.assertEqual(data[0]["value"], 1)

        uri = f"{self.static_url}/test.db"
        with self.storage.data_open(data_path=uri, auto_load_uri=True) as file:
            data = file.read()
        self.assertEqual(data, b"")
