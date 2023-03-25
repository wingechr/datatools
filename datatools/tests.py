# coding: utf-8
import logging
import os
import tempfile
import unittest

logging.basicConfig(level=logging.INFO)


from . import Repository
from .utils import filecache_json, get_local_path


class TestRepository(unittest.TestCase):
    def test_frozen(self):
        """cannot assign attributes"""
        repo = Repository()
        uri = "http://uri"
        res = repo[uri]
        self.assertEqual(res.uri, uri)
        self.assertRaises(Exception, setattr, res, "uri", "new_uri")


class TestFunctionFileCache(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        cls.tmpdir = cls._tmpdir.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls._tmpdir.__exit__(None, None, None)

    # EXAMPLE
    def test_filecache_json(self):
        class CreateList:
            """call only once"""

            def __init__(self):
                self.called = False

            def __call__(self, val):
                assert not self.called
                self.called = True
                return [val]

        create_list = CreateList()

        filepath = self.tmpdir + "/test_filecache_json.json"
        self.assertFalse(os.path.exists(filepath))

        val = 10

        # first call
        data = filecache_json(filepath, create_list, val)
        self.assertListEqual([val], data)
        self.assertTrue(os.path.exists(filepath))

        # second call
        data = filecache_json(filepath, create_list, val)
        self.assertListEqual([val], data)


class TestPaths(unittest.TestCase):
    def test_get_local_path(self):
        self.assertEqual(get_local_path("", ""), "")
