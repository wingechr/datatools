# coding: utf-8
import logging
import subprocess as sp
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread

from datatools.exceptions import DataDoesNotExists, DataExists, InvalidPath
from datatools.storage import (
    HASHED_DATA_PATH_PREFIX,
    LocalStorage,
    RemoteStorage,
    Storage,
    StorageServer,
    StorageTestCli,
)
from datatools.utils import get_free_port, normalize_path, path_to_file_uri

from . import objects_euqal

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
)


class TestLocalStorage(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.storage = LocalStorage(location=self.tempdir.__enter__())

    def tearDown(self) -> None:
        self.tempdir.__exit__(None, None, None)

    def test_storage(self):
        # create local instance in temporary dir
        data = b"hello world"
        data_path_user = "/My/path"
        invalid_path = HASHED_DATA_PATH_PREFIX + "my/path"

        # cannot save save data to hash subdir
        self.assertRaises(
            InvalidPath, self.storage.data_put, data=data, data_path=invalid_path
        )
        # save data without path
        data_path = self.storage.data_put(data=data)
        self.assertTrue(data_path.startswith(HASHED_DATA_PATH_PREFIX))

        # save data
        data_path = self.storage.data_put(data=data, data_path=data_path_user)
        self.assertEqual(normalize_path(data_path_user), data_path)
        # save again will fail
        self.assertRaises(
            DataExists, self.storage.data_put, data=data, data_path=data_path_user
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

        # get all metadata (in a list)
        metadata2 = self.storage.metadata_get(data_path=data_path_user)
        self.assertTrue(
            objects_euqal(metadata2, {"a": [1, 2, 3], "b": {"c": ["test", "test2"]}})
        )

        # get partial
        metadata2 = self.storage.metadata_get(
            data_path=data_path_user, metadata_path="b.c"
        )
        self.assertTrue(objects_euqal(metadata2, ["test", "test2"]))


class TestRemoteStorage(TestLocalStorage):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        port = get_free_port()
        storage = Storage(location=self.tempdir.__enter__())
        self.server = StorageServer(storage=storage, port=port)
        self.server_thread = Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.storage = RemoteStorage(location=f"http://localhost:{port}")

    def tearDown(self) -> None:
        self.tempdir.__exit__(None, None, None)


class TestStorageTestCli(TestLocalStorage):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.location = self.tempdir.__enter__()
        port = get_free_port()
        self.storage = StorageTestCli(location=f"http://localhost:{port}")
        self.server_proc = self.storage.serve(location=self.location, port=port)
        # test static server
        self.port = get_free_port()
        self.http_proc = sp.Popen(
            [
                "python",
                "-m",
                "http.server",
                str(self.port),
                "--directory",
                self.location,
            ]
        )

    def tearDown(self) -> None:
        # shutdown server
        self.server_proc.terminate()  # or kill
        self.server_proc.wait()

        self.http_proc.terminate()  # or kill
        self.http_proc.wait()

        # FIXME: ResourceWarning: subprocess 25460 is still running
        self.tempdir.__exit__(None, None, None)

    def test_cli_read_uri(self):
        # create file
        filename = "test.txt"
        filepath = self.location + "/" + filename
        data = b"hello world"
        with open(filepath, "wb") as file:
            file.write(data)

        # read file://
        expected_path = normalize_path(path_to_file_uri(Path(filepath).absolute()))
        data_path = self.storage.data_put(data=filepath)
        self.assertEqual(expected_path, data_path)

        # read http://
        url = f"http://localhost:{self.port}/{filename}#.anchor"
        expected_path = "http/localhost/test.txt.anchor"
        data_path = self.storage.data_put(data=url)
        self.assertEqual(expected_path, data_path)
