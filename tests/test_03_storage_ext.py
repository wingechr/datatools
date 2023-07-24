# coding: utf-8
import logging
import subprocess as sp
import sys
from pathlib import Path
from threading import Thread

from datatools.storage import RemoteStorage, StorageServer, _TestCliStorage
from datatools.utils import (
    LOCALHOST,
    get_free_port,
    normalize_path,
    path_to_file_uri,
    wait_for_server,
)

from .test_02_storage import Test_01_LocalStorage

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
)


class Test_02_RemoteStorage(Test_01_LocalStorage):
    def setUp(self) -> None:
        super().setUp()

        port = get_free_port()
        remote_location = f"http://{LOCALHOST}:{port}"
        server = StorageServer(storage=self.storage, port=port)
        Thread(target=server.serve_forever, daemon=True).start()
        wait_for_server(remote_location)

        self.storage = RemoteStorage(location=remote_location)


class Test_03_TestCliStorage(Test_01_LocalStorage):
    def setUp(self) -> None:
        super().setUp()

        # use self.storage from super() to get temp dir location
        server_storage = _TestCliStorage(location=self.tempdir_path)
        port = get_free_port()
        remote_location = f"http://{LOCALHOST}:{port}"

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
        wait_for_server(f"http://{LOCALHOST}:{self.static_port}")

        # create client
        self.storage = _TestCliStorage(location=remote_location)

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
        url = f"http://user:passwd@{LOCALHOST}:{self.static_port}/{filename}#.anchor"
        expected_path = f"http/{LOCALHOST}/test.txt.anchor"
        data_path = self.storage.data_put(data=url)
        self.assertEqual(expected_path, data_path)

        # this should auto save the source
        source = self.storage.metadata_get(
            data_path=data_path, metadata_path="source.path"
        )
        # url without credentials
        exp_source = f"http://{LOCALHOST}:{self.static_port}/{filename}#.anchor"
        self.assertEqual(source, exp_source)

        print(self.storage.metadata_get(data_path=data_path))
