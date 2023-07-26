# import logging

from datatools.cache import DEFAULT_FROM_BYTES
from datatools.load import read_uri

from .test_02_storage import TestBase


class Test_05_Sql(TestBase):
    def test_read_uri_sql(self):
        query = "select cast(1 as int) as value;"
        # uri = f"mssql+pyodbc://sqldaek2?q={query}"

        # try in memory sqlite3 database
        uri = f"sqlite:///:memory:?q={query}"
        data, _metadata = read_uri(uri)
        df = DEFAULT_FROM_BYTES(data)
        self.assertEqual(df.iloc[0, 0], 1)

        # try sqlite3 database
        dbpath = self.tempdir_path.replace("\\", "/") + "/test.db"
        uri = f"sqlite:///{dbpath}?q={query}"
        data, _metadata = read_uri(uri)
        df = DEFAULT_FROM_BYTES(data)
        self.assertEqual(df.iloc[0, 0], 1)
