import hashlib
import os
import sqlite3
import subprocess as sp
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from datatools.constants import DEFAULT_HASH_METHOD
from datatools.exceptions import DataExists
from datatools.storage import StorageTemp
from datatools.utils import (
    as_uri,
    filepath_abs_to_uri,
    get_free_port,
    get_sqlite_query_uri,
    json_dumps,
    platform_is_unix,
    wait_for_server,
)

from . import objects_euqal


class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # set up static file server
        cls.__tmpdir_data = TemporaryDirectory()
        cls._tmpdir_data = cls.__tmpdir_data.__enter__()  # exit_stack.enter_context()
        port = get_free_port()
        cls._static_url = f"http://localhost:{port}"
        cls._http_proc = sp.Popen(
            [
                sys.executable,
                "-m",
                "http.server",
                str(port),
                "--directory",
                cls._tmpdir_data,
            ],
            stdout=sp.DEVNULL,  # do not show server startup message
            stderr=sp.DEVNULL,  # do not show server request logging
        )
        wait_for_server(cls._static_url)

    @classmethod
    def get_filepath(cls, name) -> str:
        return os.path.realpath(cls.__tmpdir_data.name + "/" + name)

    @classmethod
    def get_url(cls, name) -> str:
        return cls._static_url + "/" + name

    def setUp(self) -> None:
        self.storage = StorageTemp().__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._http_proc.terminate()  # or kill
        cls._http_proc.wait()
        cls.__tmpdir_data.__exit__(None, None, None)

    def tearDown(self) -> None:
        self.storage.__exit__(None, None, None)


class TestLocalStorage(TestBase):
    def test_storage(self):
        # create local instance in temporary dir

        bdata = b"hello world"
        data_path_user = "My/Path.txt"

        res = self.storage.resource((lambda: bdata), name=data_path_user)
        self.assertFalse(res.exists())

        # save data
        res.save()
        self.assertTrue(res.exists())
        self.assertEqual(res.name, "my/path.txt")

        # save again will fail
        self.assertRaises(DataExists, res.save)

        # read data
        _data = res.load(data_type=bytes)
        self.assertEqual(bdata, _data)

        # delete (twice, which is allowed
        res.delete()

        # save again
        res.save()

        # save metadata
        metadata = {"a": [1, 2, 3], "b.c[0]": "test"}
        res.metadata.update(metadata)

        # update metadata
        metadata = {"b.c[1]": "test2"}
        res.metadata.update(metadata)

        # get metadata
        metadata_b_c = res.metadata.query("b.c")

        self.assertTrue(objects_euqal(metadata_b_c, ["test", "test2"]), metadata_b_c)
        metadata_all = res.metadata.query()
        self.assertTrue(
            metadata_all["hash"][DEFAULT_HASH_METHOD],
            getattr(hashlib, DEFAULT_HASH_METHOD)(bdata).hexdigest,
        )
        self.assertTrue(metadata_all["size"], len(bdata))

    def test_encode_data_metadata(self):
        # TODO: test closure,function,partial

        data = {"c1": [1, 2, 3]}

        json_res = self.storage.resource(
            (lambda: data), name="test_encode_data_metadata.json"
        )
        json_res.save()
        data2 = json_res.load(data_type=pd.DataFrame)
        self.assertDictEqual(data, data2)

        df = pd.DataFrame(data)
        pkl_res = self.storage.resource(
            (lambda: df), name="test_encode_data_metadata.pickle"
        )
        pkl_res.save()
        df2 = pkl_res.load()
        pd.testing.assert_frame_equal(df, df2)

        csv_res = self.storage.resource(
            (lambda: df), name="test_encode_data_metadata.csv"
        )
        csv_res.save()
        df2 = csv_res.load(data_type=pd.DataFrame)
        pd.testing.assert_frame_equal(df, df2)

    def test_resource_sqlite_memory(self):
        # in memory sqlite3 database
        query = "select cast(101 as int) as value;"
        uri = get_sqlite_query_uri(
            location=None, sql_query=query, fragment_name="/testquery.json"
        )
        res = self.storage.resource(uri)
        data = res.load()
        self.assertEqual(data[0]["value"], 101)

    def test_resource_sqlite_file(self):
        # sqlite file
        db_filepath = self.get_filepath("test.db")
        if platform_is_unix():
            db_filepath = "/" + db_filepath

        con = sqlite3.connect(db_filepath)
        cur = con.cursor()
        cur.execute("create table test(value int);")
        cur.execute("insert into test values(102);")
        cur.close()
        con.commit()
        con.close()

        query = "select value from test"
        uri = get_sqlite_query_uri(
            location=db_filepath, sql_query=query, fragment_name="/testquery.pickle"
        )

        res = self.storage.resource(uri)
        data = res.load()

        self.assertEqual(data[0]["value"], 102)

    def test_resource_file(self):
        # create files in static dir
        fpath = self.get_filepath("testfile.json")
        with open(fpath, "w", encoding="utf-8") as file:
            file.write(json_dumps({"value": 103}, ensure_ascii=False))

        # load from path
        uri = filepath_abs_to_uri(Path(fpath))
        res = self.storage.resource(uri)
        data = res.load()
        self.assertEqual(data["value"], 103)

    def test_resource_http(self):
        # create files in static dir
        fpath = self.get_filepath("testfile.json")
        with open(fpath, "w", encoding="utf-8") as file:
            file.write(json_dumps({"value": 103}, ensure_ascii=False))

        # load from webserver
        uri = self.get_url("testfile.json")
        res = self.storage.resource(uri)
        data = res.load()
        self.assertEqual(data["value"], 103)

    def test_disallow_filepath_in_storage(self):
        """When using file storage, a file:// source must not
        be inside the storage location
        """
        storage_uri = as_uri(self.storage._location)
        file_uri = storage_uri + "test"
        self.assertRaises(Exception, self.storage.resource, file_uri)
