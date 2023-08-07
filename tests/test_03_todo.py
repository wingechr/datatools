import logging

from datatools.cache import DEFAULT_FROM_BYTES
from datatools.exceptions import DataDoesNotExists
from datatools.load import open_uri
from datatools.utils import platform_is_unix

from .test_02_storage import TestBase


class Test_04_Cache(TestBase):
    def __DISABLED__test_cache_decorator(self):
        context = {"counter": 0}

        @self.storage.cache(path_prefix="myproject/cache/")  # use defaults
        def test_fun_sum(a, b):
            logging.debug("running test_fun_sum")
            context["counter"] += 1
            return a + b

        self.assertEqual(context["counter"], 0)
        self.assertEqual(test_fun_sum(1, 1), 2)
        # counted up, because first try
        self.assertEqual(context["counter"], 1)
        self.assertEqual(test_fun_sum(1, 1), 2)
        # not counted up, because cache
        self.assertEqual(context["counter"], 1)
        self.assertEqual(test_fun_sum(1, 2), 3)
        # counted up, because new signature
        self.assertEqual(context["counter"], 2)


class Test_05_Sql(TestBase):
    def __DISABLED__test_read_uri_sql(self):
        query = "select cast(1 as int) as value;"
        # uri = f"mssql+pyodbc://sqldaek2?q={query}"

        # try in memory sqlite3 database
        uri = f"sqlite:///:memory:?q={query}"
        data, _metadata = open_uri(uri)
        data = DEFAULT_FROM_BYTES(data.read())
        self.assertEqual(data[0]["value"], 1)
        # self.assertEqual(df.iloc[0, 0], 1)

        # try sqlite3 database
        dbpath = self.static_dir.replace("\\", "/") + "/test.db"
        uri = f"sqlite:///{dbpath}?q={query}"
        data, _metadata = open_uri(uri)
        data = DEFAULT_FROM_BYTES(data.read())
        self.assertEqual(data[0]["value"], 1)
        # self.assertEqual(df.iloc[0, 0], 1)

    def __DISABLED__test_storage_autoload(self):
        uri = "sqlite:///:memory:?q=select 1 as value#/query1"
        self.assertRaises(DataDoesNotExists, self.storage.data_open, data_path=uri)

        with self.storage.data_open(data_path=uri, auto_load_uri=True) as file:
            data = file.read()
        data = DEFAULT_FROM_BYTES(data)
        self.assertEqual(data[0]["value"], 1)

        # save test file
        db_filepath = self.static_dir + "/test.db"
        # file should be created by sqlalchemy

        # only for sqlite:
        # in need an additional slash in linux for abs path
        if platform_is_unix:
            db_filepath = "/" + db_filepath

        uri = f"sqlite://{db_filepath}?q=select 1 as value#/query1"
        with self.storage.data_open(data_path=uri, auto_load_uri=True) as file:
            data = file.read()
        data = DEFAULT_FROM_BYTES(data)
        self.assertEqual(data[0]["value"], 1)

        uri = f"{self.static_url}/test.db"
        with self.storage.data_open(data_path=uri, auto_load_uri=True) as file:
            data = file.read()
        self.assertEqual(data, b"")
