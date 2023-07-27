import logging
import subprocess as sp
import sys
from pathlib import Path
from threading import Thread

import requests

from datatools.storage import RemoteStorage, StorageServer, _TestCliStorage
from datatools.utils import (
    LOCALHOST,
    MyBufferedReader,
    filepath_abs_to_uri,
    get_free_port,
    normalize_path,
    wait_for_server,
)

# dont import Test_01_LocalStorage directly, or it will be run twice
from . import test_02_storage as t


class Test_02_RemoteStorage(t.Test_01_LocalStorage):
    def setUp(self) -> None:
        super().setUp()

        port = get_free_port()
        remote_location = f"http://{LOCALHOST}:{port}"
        server = StorageServer(storage=self.storage, port=port)
        Thread(target=server.serve_forever, daemon=True).start()
        wait_for_server(remote_location)

        self.storage = RemoteStorage(location=remote_location)

    def test_wsgi_post_from_buffer(self):
        data = b"hello world"

        res = requests.post(
            url=self.storage.location,
            data=MyBufferedReader(data),
        )

        self.assertTrue(res.ok)


class Test_03_TestCliStorage(t.Test_01_LocalStorage):
    def setUp(self) -> None:
        super().setUp()

        # use self.storage from super() to get temp dir location
        server_storage = _TestCliStorage(location=self.tempdir_path)
        port = get_free_port()
        remote_location = f"http://{LOCALHOST}:{port}"
        logging.debug(f"storage: {remote_location}")
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
        static_location = f"http://{LOCALHOST}:{self.static_port}"
        wait_for_server(static_location)
        logging.debug(f"static: {static_location}")

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

        logging.debug("read uri from filepath")
        expected_path = normalize_path(filepath_abs_to_uri(Path(filepath).absolute()))
        # NOTE: use _data_put directly for this test
        data_path = self.storage._uri_put(
            uri=filepath, norm_data_path=None, exist_ok=False
        )
        self.assertEqual(expected_path, data_path)

        logging.debug("read uri from url")
        url = f"http://user:passwd@{LOCALHOST}:{self.static_port}/{filename}#.anchor"
        expected_path = f"http/{LOCALHOST}/test.txt.anchor"
        data_path = self.storage._uri_put(uri=url, norm_data_path=None, exist_ok=False)

        self.assertEqual(expected_path, data_path)
        logging.debug("check metadata")
        source = self.storage.metadata_get(
            data_path=data_path, metadata_path="source.path"
        )
        logging.debug("check that passwords in url are not saved")
        exp_source = f"http://{LOCALHOST}:{self.static_port}/{filename}#.anchor"
        self.assertEqual(source, exp_source)
        print(self.storage.metadata_get(data_path=data_path))
