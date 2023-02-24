# coding: utf-8

import logging
import os
import tempfile
import unittest

import datatools as dt

# from pathlib import Path

logging.basicConfig(level=logging.INFO)


class TestTemplate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._test_dir = tempfile.TemporaryDirectory()
        cls.test_dir = os.path.abspath(cls._test_dir.__enter__())

    @classmethod
    def tearDownClass(cls):
        cls._test_dir.__exit__(None, None, None)

    @classmethod
    def create_testfile(cls, relpath, byte_data=b""):
        path = cls.get_path(relpath)
        # make sure we are still in test dir
        assert path.startswith(cls.test_dir)
        logging.debug(f"creating file {path}")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as file:
            file.write(byte_data)
        return path

    @classmethod
    def get_path(cls, relpath):
        return os.path.abspath(os.path.join(cls.test_dir, relpath))

    def test_TEMPLATE(self):
        path = self.create_testfile("d1/d2/f")

        # no index exists, so we should get the the folder the file is in
        res = dt.storage.get_index_base_path(path)
        self.assertEqual(str(res), self.get_path("d1/d2"))

        self.create_testfile("d1/datapackage.json")
        # now index exists, so we should get the location of that
        res = dt.storage.get_index_base_path(path)
        self.assertEqual(res, self.get_path("d1"))

        # in a zip file
        res = dt.storage.get_index_base_path(self.get_path("x/y/p.zip/d1/f"))
        self.assertEqual(res, self.get_path("x/y/p.zip"))

        idx_loc = self.get_path("")
        with dt.storage.FolderIndex(idx_loc) as idx:
            print(idx.data)

        idx_loc = self.get_path("test.zip")
        with dt.storage.ZipFileIndex(idx_loc) as idx:
            print(idx.data)
