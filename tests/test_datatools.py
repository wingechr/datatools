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
        self.df_test = pd.DataFrame([{"value": 1}, {"value": 2}])

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
        resources = []

        for uri, name in [
            (self.test_uri_file, "extern/test.csv"),
            (self.test_uri_http, "extern/test.xlsx"),
            (self.test_uri_sql, "extern/test.json"),
        ]:
            resource1 = storage.resource(name)
            process = Process.from_uri(uri)

            process(resource1)
            df1 = resource1.load(datatype=pd.DataFrame)
            self.assertTrue(isinstance(df1, pd.DataFrame))

            # alternatively: dump directly into storage
            resource2 = cast(Resource, process(storage))
            df2 = resource2.load(datatype=pd.DataFrame)
            self.assertTrue(isinstance(df2, pd.DataFrame))
            resources.append(resource2)

            pd.testing.assert_frame_equal(df1, df2)

        # set index_col manually in metadata
        # so it will be available when opening
        resources[0].metadata.set(index_col=0)
        resources[1].metadata.set(index_col=0)

        # now, use resources to create a new one
        def test_combine_dfs(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
            return df1 + df2

        process = Function(test_combine_dfs).process(resources[0], resources[1])
        resource = storage.resource("result/test.xlsx")
        process(resource)

        df = resource.load()
        pd.testing.assert_frame_equal(self.df_test * 2, df)


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
