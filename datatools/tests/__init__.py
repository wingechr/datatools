# coding: utf-8
import logging
import tempfile
import unittest

from click.testing import CliRunner

from datatools.__main__ import main
from datatools.classes import (
    DictMixin,
    LazyJsonResource,
    MainContext,
    SharedContext,
    SharedInstance,
    with_read,
    with_write,
)


class TestSharedInstance(SharedInstance):
    def __init__(self, obj_id, data=None):
        pass

    @classmethod
    def _get_instance_id(cls, obj_id, *args, **kwargs):
        return obj_id


class TestSharedContext(SharedContext):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self._data = None

    def _open(self):
        assert self._data is None  # open should only be called once
        self._data = {"k1": "v1"}

    def _close(self, *args, **kwargs):
        self._data = None

    @with_read
    def action_read(self, k):
        return self._data[k]

    @with_write
    def action_write(self, k, v):
        self._data[k] = v


class TestSharedContextInstance(TestSharedContext, SharedInstance, DictMixin):
    def __init__(self, inst_id, parent=None):
        super().__init__(parent=parent)
        self._data = None

    @classmethod
    def _get_instance_id(cls, inst_id, *args, **kwargs):
        return inst_id


logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


class TestClasses(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        cls.pwd = cls.tmpdir.__enter__()
        logging.info(cls.pwd)

    @classmethod
    def tearDownClass(cls):
        cls.tmpdir.__exit__(None, None, None)

    def test_shared_instance(self):
        # test simple shareing class
        self.assertEqual(TestSharedInstance(1), TestSharedInstance(1))
        self.assertNotEqual(TestSharedInstance(1), TestSharedInstance(2))

    def test_shared_context(self):
        # test simple shareing class

        v = TestSharedContext()
        # this does not work: no context
        self.assertRaises(Exception, v.action_read, "k1")

        # 1) use context directly
        with v:
            self.assertEqual(v.action_read("k1"), "v1")

        # use a parent context
        with MainContext() as main:
            v = TestSharedContext(main)
            self.assertEqual(v.action_read("k1"), "v1")

        # entering multiple times is not a problem
        # use a parent context
        with MainContext() as main:
            v = TestSharedContext(main)
            with v:
                with v:
                    self.assertEqual(v.action_read("k1"), "v1")

        # and now:share the resource

    def test_shared_context_2(self):
        with MainContext() as main:
            v_a = TestSharedContextInstance("A", parent=main)
            v_a["x"] = 1
            logging.info(v_a._is_open)
            with TestSharedContextInstance("A") as v_a_2:
                self.assertEqual(v_a, v_a_2)
                self.assertEqual(v_a_2["x"], 1)

    def test_LazyJsonResource(self):
        path = self.pwd + "/test.json"
        with LazyJsonResource(path) as j1:
            # j2 = LazyJsonResource(path) # same as j1
            j1["x"] = j1.get("x", "999")

        j2 = LazyJsonResource(path)  # same as j1
        j2["x"]


class TestCli(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory()
        cls.pwd = cls.tmpdir.__enter__()
        logging.info(cls.pwd)
        # os.chdir(cls.pwd) # creates error in windows
        cls.cli_runner = CliRunner(mix_stderr=False)

    @classmethod
    def tearDownClass(cls):
        cls.tmpdir.__exit__(None, None, None)

    def call(self, cmd):
        cmd = ["--base-dir", self.pwd] + cmd
        logging.debug(" ".join(["dt"] + cmd))
        result = self.cli_runner.invoke(main, cmd)
        self.assertEqual(result.exit_code, 0, result.stderr)
        output = result.stdout.rstrip("\n")  # remove newline
        return output

    def test_basic(self):
        # get default
        result = self.call(["meta", "test_file.dat", "get", "key1", "default_val1"])
        self.assertEqual(result, "default_val1")

        # persistency
        self.cli_runner.invoke(main, ["meta", "test_file.dat", "set", "key1", "val1"])
        result = self.call(["meta", "test_file.dat", "get", "key1"])
        self.assertEqual(result, "val1")
