import unittest
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread

import pandas as pd

from datatools.base import PARAM_SQL_QUERY
from datatools.process import Process
from datatools.storage import Storage
from datatools.utils import (
    filepath_abs_to_uri,
    get_free_port,
    get_hostname,
    import_module_from_path,
)


class TestDatatoolsExample(unittest.TestCase):

    def setUp(self):
        # create temp dir that will be deleted after end of test
        self.tempdir = TemporaryDirectory()

        # test data
        self.df_test = pd.DataFrame([{"key": 1}, {"key": 2}])

        # create csv file
        test_csv_path = self.tempdir.name + "/test.csv"
        self.test_uri_file = filepath_abs_to_uri(Path(test_csv_path).absolute())
        self.df_test.to_csv(test_csv_path)
        self.df_test.to_excel(test_csv_path.replace(".csv", ".xlsx"))

        # add webserver that will serve tempdir
        host = "localhost"  # get_hostname()  # "localhost"
        port = get_free_port()
        handler_class = partial(SimpleHTTPRequestHandler, directory=self.tempdir.name)
        self.test_uri_http = f"http://{host}:{port}/test.xlsx"
        httpd = HTTPServer((host, port), handler_class)
        server_thread = Thread(target=httpd.serve_forever, daemon=True)
        server_thread.start()

        # add database
        db_path = Path(self.tempdir.name + "/test.sqlite3").as_posix()
        self.test_uri_sql = f"sqlite:///{db_path}?{PARAM_SQL_QUERY}=select * from test"
        self.df_test.to_sql("test", self.test_uri_sql)

    def tearDown(self):
        self.tempdir.cleanup()
        # server thread and memory database will be cleaned up automatically

    def test_datatools_example(self):
        # create project storage
        storage = Storage(self.tempdir.name + "/__data__")

        # auto import remote data from file, database, website
        # into storage
        for uri, name in [
            (self.test_uri_file, "extern/test.csv"),
            (self.test_uri_http, "extern/test.xlsx"),
            (self.test_uri_sql, "extern/test.json"),
        ]:
            resource = storage.resource(name)
            process = Process.from_uri(uri)
            process(resource)

            print(resource.get_loader(pd.DataFrame)())


class TestDatatoolsDocs(unittest.TestCase):
    def test_datatools_example_docs(self):
        import_module_from_path("example", "docs/example.py")
