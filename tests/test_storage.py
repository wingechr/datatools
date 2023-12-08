import hashlib
import json
import logging
import os
import pickle
import sqlite3
import subprocess as sp
import sys
import unittest
from tempfile import TemporaryDirectory

from datatools.constants import DEFAULT_HASH_METHOD
from datatools.exceptions import DataDoesNotExists, DataExists
from datatools.storage import Storage
from datatools.utils import (
    get_free_port,
    make_file_writable,
    platform_is_unix,
    wait_for_server,
)

from . import objects_euqal


class MyTemporaryDirectory(TemporaryDirectory):
    def __init__(self, *args, **kwargs):
        self.tempdir = TemporaryDirectory(*args, **kwargs)

    def __enter__(self):
        return self.tempdir.__enter__()

    def __exit__(self, *args):
        # make files writable so cleanup can delete them
        path = self.tempdir.name
        for rt, _ds, fs in os.walk(path):
            for f in fs:
                filepath = f"{rt}/{f}"
                make_file_writable(filepath)
                logging.debug(f"FILE: {filepath}")
        # delete dir
        self.tempdir.__exit__(*args)
        logging.debug(f"DELETING {path}: {not os.path.exists(path)}")


class TestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir1 = MyTemporaryDirectory()
        path_tempdir1 = self.tempdir1.__enter__()  # exit_stack.enter_context()
        self.storage = Storage(location=path_tempdir1)

        # set up static file server
        self.tempdir2 = TemporaryDirectory()
        self.static_dir = self.tempdir2.__enter__()  # exit_stack.enter_context()
        port = get_free_port()
        self.static_url = f"http://localhost:{port}"
        self.http_proc = sp.Popen(
            [
                sys.executable,
                "-m",
                "http.server",
                str(port),
                "--directory",
                self.static_dir,
            ],
            stdout=sp.DEVNULL,  # do not show server startup message
            stderr=sp.DEVNULL,  # do not show server request logging
        )
        wait_for_server(self.static_url)

    def tearDown(self) -> None:
        self.http_proc.terminate()  # or kill
        self.http_proc.wait()

        self.tempdir1.__exit__(None, None, None)
        self.tempdir2.__exit__(None, None, None)


class TestLocalStorage(TestBase):
    def test_storage(self):
        # create local instance in temporary dir

        data = b"hello world"
        data_path_user = "data:///My/path"

        res = self.storage.resource(data_path_user)
        self.assertFalse(res.exists())

        logging.debug("save data")
        res.write(data)
        self.assertTrue(res.exists())
        self.assertEqual(res.name, "my/path")

        logging.debug("save again will fail")
        self.assertRaises(DataExists, res.write, data)

        logging.debug("read data")
        with res.open() as file:
            _data = file.read()
        self.assertEqual(data, _data)

        logging.debug("delete (twice, which is allowed)")
        res.delete()
        # reading now will raise error
        self.assertRaises((DataDoesNotExists, NotImplementedError), res.open)
        res.delete()

        logging.debug("save again")
        res.write(data)

        logging.debug("save metadata")
        metadata = {"a": [1, 2, 3], "b.c[0]": "test"}
        res.metadata.update(metadata)

        logging.debug("update metadata")
        metadata = {"b.c[1]": "test2"}
        res.metadata.update(metadata)

        logging.debug("get metadata")

        metadata_b_c = res.metadata.get("b.c")
        self.assertTrue(objects_euqal(metadata_b_c, ["test", "test2"]), metadata_b_c)
        metadata_all = res.metadata.get()
        self.assertTrue(
            metadata_all["hash"][DEFAULT_HASH_METHOD],
            getattr(hashlib, DEFAULT_HASH_METHOD)(data).hexdigest,
        )
        self.assertTrue(metadata_all["size"], len(data))

    def test_cache_decorator(self):
        context = {"counter": 0}

        @self.storage.cache()
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


class TestResource(TestBase):
    def test_resource(self):
        # in memory sqlite3 database
        query = "select cast(101 as int) as value;"
        uri = f"sqlite:///:memory:?q={query}#/testquery.pickle"
        res = self.storage.resource(uri)
        with res.open() as file:
            bdata = file.read()
        data = pickle.loads(bdata)
        self.assertEqual(data[0]["value"], 101)

        # sqlite file
        db_filepath = self.static_dir + "/test.db"

        con = sqlite3.connect(db_filepath)
        cur = con.cursor()
        cur.execute("create table test(value int);")
        cur.execute("insert into test values(102);")
        cur.close()
        con.commit()
        con.close()

        # file should be created by sqlalchemy
        # only for sqlite:
        # in need an additional slash in linux for abs path
        if platform_is_unix:
            db_filepath = "/" + db_filepath
        uri = f"sqlite://{db_filepath}?q=select value from test#/testquery.pickle"

        res = self.storage.resource(uri)
        with res.open() as file:
            bdata = file.read()
        data = pickle.loads(bdata)

        self.assertEqual(data[0]["value"], 102)

        # create files in static dir
        fpath = os.path.join(self.static_dir, "testfile.json")
        with open(fpath, "w", encoding="utf-8") as file:
            json.dump({"value": 103}, file, ensure_ascii=False)

        # load from path
        uri = fpath
        res = self.storage.resource(uri)
        with res.open() as file:
            data = json.load(file)
        self.assertEqual(data["value"], 103)

        # load from webserver

        uri = self.static_url + "/testfile.json"
        res = self.storage.resource(uri)
        with res.open() as file:
            data = json.load(file)
        self.assertEqual(data["value"], 103)
