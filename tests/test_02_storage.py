import hashlib
import logging
import os
import subprocess as sp
import sys

# import secrets
import unittest
from tempfile import TemporaryDirectory

from datatools.constants import DEFAULT_HASH_METHOD
from datatools.exceptions import DataDoesNotExists, DataExists
from datatools.storage import Storage
from datatools.utils import get_free_port, make_file_writable, wait_for_server

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

        data = b"hello world"
        data_path_user = "/My/path"

        self.assertFalse(self.storage.data_exists(data_path_user))

        logging.debug("save data")
        data_path = self.storage.data_put(data=data, data_path=data_path_user)
        self.assertTrue(self.storage.data_exists(data_path_user))
        self.assertEqual(data_path, "my/path")

        logging.debug("save again will fail")
        self.assertRaises(
            DataExists,
            self.storage.data_put,
            data=data,
            data_path=data_path_user,
        )

        logging.debug("read data")
        with self.storage.data_open(data_path=data_path_user) as file:
            _data = file.read()
        self.assertEqual(data, _data)

        logging.debug("delete (twice, which is allowed)")
        self.storage.data_delete(data_path=data_path_user)
        # reading now will raise error
        self.assertRaises(
            DataDoesNotExists, self.storage.data_open, data_path=data_path_user
        )
        self.storage.data_delete(data_path=data_path_user)

        logging.debug("save again")
        self.storage.data_put(data=data, data_path=data_path_user)

        logging.debug("save metadata")
        metadata = {"a": [1, 2, 3], "b.c[0]": "test"}
        self.storage.metadata_set(data_path=data_path_user, metadata=metadata)

        logging.debug("update metadata")
        metadata = {"b.c[1]": "test2"}
        self.storage.metadata_set(data_path=data_path_user, metadata=metadata)

        logging.debug("get metadata")
        metadata_b_c = self.storage.metadata_get(
            data_path=data_path_user, metadata_path="b.c"
        )
        self.assertTrue(objects_euqal(metadata_b_c, ["test", "test2"]))
        metadata_all = self.storage.metadata_get(data_path=data_path_user)
        self.assertTrue(
            metadata_all["hash"][DEFAULT_HASH_METHOD],
            getattr(hashlib, DEFAULT_HASH_METHOD)(data).hexdigest,
        )
        self.assertTrue(metadata_all["size"], len(data))
