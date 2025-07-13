import unittest
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from threading import Thread
from typing import Any, cast

import pandas as pd

from datatools.base import PARAM_SQL_QUERY
from datatools.process import Function, Process
from datatools.storage import Resource, Storage
from datatools.utils import (
    filepath_abs_to_uri,
    get_free_port,
    get_hostname,
    import_module_from_path,
)

from . import TestDatatoolsTempdir


class HTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_request(self, *args: Any, **kwargs: Any) -> None:
        # dont log requests im tests
        pass


class TestDatatoolsExample(TestDatatoolsTempdir):

    def setUp(self):
        super().setUp()

        # test data
        self.df_test = pd.DataFrame([{"key": 1}, {"key": 2}])

        # create csv file
        test_csv_path = self.tempdir.name + "/test.csv"
        self.test_uri_file = filepath_abs_to_uri(Path(test_csv_path).absolute())
        self.df_test.to_csv(test_csv_path)
        self.df_test.to_excel(test_csv_path.replace(".csv", ".xlsx"))  # type:ignore

        # add webserver that will serve tempdir
        host = get_hostname()
        port = get_free_port()
        handler_class = partial(HTTPRequestHandler, directory=self.tempdir.name)
        self.test_uri_http = f"http://{host}:{port}/test.xlsx"
        httpd = HTTPServer((host, port), handler_class)
        server_thread = Thread(target=httpd.serve_forever, daemon=True)
        server_thread.start()

        # add database

        db_path = Path(self.tempdir.name + "/test.sqlite3").as_posix()
        # add test.json fragment to indicate output filename
        self.test_uri_sql = (
            f"sqlite:///{db_path}?{PARAM_SQL_QUERY}=select * from test#test.json"
        )
        self.df_test.to_sql("test", self.test_uri_sql)

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
            df = resource.load(datatype=pd.DataFrame)
            self.assertTrue(isinstance(df, pd.DataFrame))

            # alternatively: dump directly into storage
            resource = cast(Resource, process(storage))
            df = resource.load(datatype=pd.DataFrame)
            self.assertTrue(isinstance(df, pd.DataFrame))


class TestDatatoolsProcessStorage(TestDatatoolsTempdir):

    def test_datatools_proceess_resource(self):

        storage = Storage(self.tempdir.name)

        res_inp = storage.resource("input.json")
        res_outp = storage.resource("output.json")

        res_inp.dump([1, 2, 3])

        def function(data: list[int], factor: int) -> list[int]:
            return data * factor

        func = Function(function=function)
        proc = func.process(res_inp, 10)

        self.assertFalse(res_outp.exist())
        proc(res_outp)
        self.assertTrue(res_outp.exist())
        # cannot run process again, because resource already exists
        self.assertRaises(Exception, proc, res_outp)

    def test_datatools_proceess_storage(self):

        storage = Storage(self.tempdir.name)

        res_inp = storage.resource("input.json")
        res_inp.dump([1, 2, 3])

        def function(data: list[int], factor: int) -> list[int]:
            return data * factor

        function = Function(function=function)
        process = function.process(res_inp, 10)

        res_outp = process(storage)
        self.assertTrue(isinstance(res_outp, Resource))
        self.assertTrue(cast(Resource, res_outp).exist())

    def test_datatools_proceess_uri(self):
        storage = Storage(self.tempdir.name)

        uri = "http://example.com#/index.html"
        process = Process.from_uri(uri)

        res_outp = process(storage)
        self.assertTrue(isinstance(res_outp, Resource))
        self.assertTrue(cast(Resource, res_outp).exist())


class TestDatatoolsDocs(unittest.TestCase):
    def test_datatools_docs(self):
        import_module_from_path("example", "docs/example.py")
