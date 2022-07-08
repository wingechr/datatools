"""Test all schemata"""
import os
from functools import partial
from test import JSONSCHEMA_JSON, SCHEMA_DIR, TestCase

from datatools.utils.json import SCHEMA_SUFFIX, Validator, load, load_schema, validate


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
            json = load(fp)
            validate(json)

    def test_load_schema_file(self):
        file_uri = f"{SCHEMA_DIR}/{JSONSCHEMA_JSON}"
        schema = load_schema(file_uri)
        validate(schema, schema)


class TestValidate(TestCase):
    def test_validate(self):
        _ = partial(Validator())
        pass
