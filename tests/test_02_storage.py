# coding: utf-8
import logging
import os
import subprocess as sp
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread

from datatools.exceptions import DataDoesNotExists, DataExists, InvalidPath
from datatools.storage import (
    HASHED_DATA_PATH_PREFIX,
    LocalStorage,
    RemoteStorage,
    StorageServer,
    TestCliStorage,
)
from datatools.utils import (
    get_free_port,
    make_file_writable,
    normalize_path,
    path_to_file_uri,
    wait_for_server,
)

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


class Test_02_RemoteStorage(Test_01_LocalStorage):
    def setUp(self) -> None:
        super().setUp()

        port = get_free_port()
        remote_location = f"http://localhost:{port}"
        server = StorageServer(storage=self.storage, port=port)
        Thread(target=server.serve_forever, daemon=True).start()
        wait_for_server(remote_location)

        self.storage = RemoteStorage(location=remote_location)


class Test_03_TestCliStorage(Test_01_LocalStorage):
    def setUp(self) -> None:
        super().setUp()

        # use self.storage from super() to get temp dir location
        server_storage = TestCliStorage(location=self.tempdir_path)
        port = get_free_port()
        remote_location = f"http://localhost:{port}"

        self.server_proc = server_storage.serve(port=port)
        wait_for_server(remote_location)

        # test static server process (to serve test files)
        self.static_port = get_free_port()
        self.http_proc = sp.Popen(
            [
                sys.executable,
                "-m",
                "http.server",
                str(self.static_port),
                "--directory",
                self.tempdir_path,
            ]
        )
        wait_for_server(f"http://localhost:{self.static_port}")

        # create client
        self.storage = TestCliStorage(location=remote_location)

    def tearDown(self) -> None:
        # shutdown server
        self.server_proc.terminate()  # or kill
        self.server_proc.wait()

        self.http_proc.terminate()  # or kill
        self.http_proc.wait()

        super().tearDown()

    def test_cli_read_uri(self):
        # create file
        filename = "test.txt"
        filepath = self.tempdir_path + "/" + filename
        data = b"hello world"
        with open(filepath, "wb") as file:
            file.write(data)

        # read file://
        expected_path = normalize_path(path_to_file_uri(Path(filepath).absolute()))
        data_path = self.storage.data_put(data=filepath)
        self.assertEqual(expected_path, data_path)

        # read http://
        url = f"http://user:passwd@localhost:{self.static_port}/{filename}#.anchor"
        expected_path = "http/localhost/test.txt.anchor"
        data_path = self.storage.data_put(data=url)
        self.assertEqual(expected_path, data_path)

        # this should auto save the source
        source = self.storage.metadata_get(
            data_path=data_path, metadata_path="source.path"
        )
        # url without credentials
        exp_source = f"http://localhost:{self.static_port}/{filename}#.anchor"
        self.assertEqual(source, exp_source)

        print(self.storage.metadata_get(data_path=data_path))
