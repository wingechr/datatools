# coding: utf-8

import unittest
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread

import pandas as pd

from datatools.process import Process
from datatools.storage import Storage
from datatools.utils import filepath_abs_to_uri, get_free_port, import_module_from_path


class TestDatatoolsExample(unittest.TestCase):

    def setUp(self):
        # create temp dir that will be deleted after end of test
        self.tempdir = TemporaryDirectory()

        # test data
        self.df_test = pd.DataFrame([{"key": 1}, {"key": 2}])

        # create csv file
        self.test_csv_path = self.tempdir.name + "/test.csv"
        self.test_csv_uri = filepath_abs_to_uri(Path(self.test_csv_path).absolute())
        self.df_test.to_csv(self.test_csv_path)

        # add webserver that will serve tempdir
        host = "localhost"
        port = get_free_port()
        handler_class = partial(SimpleHTTPRequestHandler, directory=self.tempdir.name)
        self.test_csv_url = f"http://{host}:{port}/test.csv"
        httpd = HTTPServer((host, port), handler_class)
        server_thread = Thread(target=httpd.serve_forever, daemon=True)
        server_thread.start()

        # add database
        db_path = Path(self.tempdir.name + "/test.sqlite3").as_posix()
        self.test_db_url = f"sqlite:///{db_path}"  # host must be empty
        self.df_test.to_sql("test", self.test_db_url)

    def tearDown(self):
        self.tempdir.cleanup()
        # server thread and memory database will be cleaned up automatically

    def test_datatools_example(self):
        # create project storage
        storage = Storage(self.tempdir.name + "__data__")
        print(storage)

        # auto import remote data
        process = Process.from_uri(self.test_csv_uri)
        print(process.metadata)


class TestDatatoolsDocs(unittest.TestCase):
    def test_datatools_example_docs(self):
        import_module_from_path("example", "docs/example.py")
