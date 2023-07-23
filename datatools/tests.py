# coding: utf-8
import json
import logging
import subprocess as sp
import unittest
from tempfile import TemporaryDirectory
from threading import Thread

from datatools.classes import (
    HASHED_DATA_PATH_PREFIX,
    LocalStorage,
    RemoteStorage,
    Storage,
    StorageServer,
    TestCliStorage,
)
from datatools.exceptions import DataDoesNotExists, DataExists, InvalidPath
from datatools.utils import get_free_port, normalize_path

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
)


def objects_euqal(left, right):
    left = json.dumps(left, sort_keys=True)
    right = json.dumps(right, sort_keys=True)
    return left == right


class TestUtils(unittest.TestCase):
    def test_normalize_data_path(self):
        for p, exp_np in [
            ("/my/path//", "my/path"),
            ("Lower  Case with SPACE ! ", "lower_case_with_space"),
            (
                "François fährt Straßenbahn zum Café Málaga",
                "francois_faehrt_strassenbahn_zum_cafe_malaga",
            ),
        ]:
            np = normalize_path(p)
            self.assertEqual(exp_np, np)
            # also: normalized path should always normalize to self
            np = normalize_path(exp_np)
            self.assertEqual(exp_np, np)


class TestDatatools(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.storage = LocalStorage(location=self.tempdir.__enter__())

    def tearDown(self) -> None:
        self.tempdir.__exit__(None, None, None)

    def test_local_instance(self):
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


class TestDatatoolsRemote(TestDatatools):
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


class TestDatatoolsRemoteCli(TestDatatools):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        port = get_free_port()
        location = self.tempdir.__enter__()
        # start server also via cls
        cmd = [
            "python",
            "-m",
            "datatools",
            "-d",
            location,
            "serve",
            "--port",
            str(port),
        ]

        self.server_proc = sp.Popen(cmd)

        # storage = Storage(location=location)
        # self.server = StorageServer(storage=storage, port=port)
        # self.server_thread = Thread(target=self.server.serve_forever, daemon=True)
        # self.server_thread.start()

        self.storage = TestCliStorage(location=f"http://localhost:{port}")

    def tearDown(self) -> None:
        self.server_proc.kill()
        # FIXME: ResourceWarning: subprocess 25460 is still running
        self.tempdir.__exit__(None, None, None)
