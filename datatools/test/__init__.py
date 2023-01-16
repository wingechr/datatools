import logging  # noqa
import os
import unittest

import datatools as dt

TEST_DATA_DIR = os.path.dirname(__file__) + "/data"

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)

# from click.testing import CliRunner

# class _TestCase(unittest.TestCase):
#    def _test_cli(self, fun_name, args):
#        runner = CliRunner()
#        fun = getattr(datatools.__main__, fun_name)
#        res = runner.invoke(fun, args, catch_exceptions=False)
#        if not res.exit_code == 0:
#            raise Exception(res.output)


class TestJsonStore(unittest.TestCase):
    def test_copy_and_deref(self):
        """make sure results are different objects"""
        store = dt.JsonStore(cache_location=TEST_DATA_DIR + "/jsonstore")
        # load and change
        data1 = store["example1/example1.json"]
        # check dereferenced value
        self.assertTrue("data" in data1["example1"])
        # change data
        data1["data"] = data1["example1"].pop("data")
        # load again
        data2 = store["example1/example1.json"]
        self.assertTrue("data" in data2["example1"])
        self.assertTrue("data" not in data2)


class TestJsonSchemaValidator(unittest.TestCase):
    def test_invalid_schema(self):
        # invalid
        self.assertRaises(Exception, dt.JsonSchemaValidator, {"type": None})

    def test_valid_instance(self):
        validator = dt.JsonSchemaValidator({"type": "integer"})
        self.assertRaises(Exception, validator, "1")
        # this works
        validator(1)
