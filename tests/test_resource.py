# import logging
import json
import os
import sqlite3

from datatools.cache import DEFAULT_FROM_BYTES
from datatools.utils import platform_is_unix

from .test_storage import TestBase


class TestResource(TestBase):
    def test_resource(self):
        # in memory sqlite3 database
        query = "select cast(101 as int) as value;"
        uri = f"sqlite:///:memory:?q={query}#/testquery"
        res = self.storage.resource(uri=uri)
        with res.open() as file:
            bdata = file.read()
        data = DEFAULT_FROM_BYTES(bdata)
        self.assertEqual(data[0]["value"], 101)

        # sqlite file
        db_filepath = self.static_dir + "/test.db"
        with sqlite3.connect(db_filepath) as con:
            cur = con.cursor()
            cur.execute("create table test(value int);")
            cur.execute("insert into test values(102);")
        # file should be created by sqlalchemy
        # only for sqlite:
        # in need an additional slash in linux for abs path
        if platform_is_unix:
            db_filepath = "/" + db_filepath
        uri = f"sqlite://{db_filepath}?q=select value from test#/testquery"
        res = self.storage.resource(uri=uri)
        with res.open() as file:
            bdata = file.read()
        data = DEFAULT_FROM_BYTES(bdata)
        self.assertEqual(data[0]["value"], 102)

        # create files in static dir
        fpath = os.path.join(self.static_dir, "testfile.json")
        with open(fpath, "w", encoding="utf-8") as file:
            json.dump({"value": 103}, file, ensure_ascii=False)

        # load from path
        res = self.storage.resource(uri=fpath)
        with res.open() as file:
            data = json.load(file)
        self.assertEqual(data["value"], 103)

        # load from webserver
        res = self.storage.resource(uri=self.static_url + "/testfile.json")
        with res.open() as file:
            data = json.load(file)
        self.assertEqual(data["value"], 103)
