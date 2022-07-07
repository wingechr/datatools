import os

from . import TestCase


class TestCLI(TestCase):
    def test_validate_json(self):
        # self validate root jsonschema.schema.json
        json_file = os.path.join("schema", "jsonschema.schema.json")
        self._test_cli("validate_json", [json_file])
