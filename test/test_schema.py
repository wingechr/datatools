"""Test all schemata"""
import os

from datatools import utils
from datatools.validate import SCHEMA_SUFFIX, load_schema, validate_json

from . import JSONSCHEMA_JSON, SCHEMA_DIR, TestCase


class TestSchema(TestCase):
    def test_schema(self):
        file_paths = []
        for rt, _, fs in os.walk(SCHEMA_DIR):
            for f in fs:
                if not f.endswith(SCHEMA_SUFFIX):
                    continue
                file_path = os.path.join(rt, f)
                file_paths.append(file_path)
        self.assertTrue(file_paths)  # must have at least one
        for fp in file_paths:
            json = utils.json.load(fp)
            validate_json(json)

    def test_load_schema_file(self):
        file_uri = f"{SCHEMA_DIR}/{JSONSCHEMA_JSON}"
        schema = load_schema(file_uri)
        validate_json(schema, schema)
