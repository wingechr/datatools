"""Test all schemata"""
import os

from datatools import utils, validate_json

from . import TestCase


class TestSchema(TestCase):
    def test_schema(self):
        filepaths = []
        for rt, _, fs in os.walk("schema"):
            for f in fs:
                if not f.endswith(".schema.json"):
                    continue
                filepath = os.path.join(rt, f)
                filepaths.append(filepath)
        self.assertTrue(filepaths)  # must have at least one
        for fp in filepaths:
            json = utils.json_load(fp)
            validate_json(json)
