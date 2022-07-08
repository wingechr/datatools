import os

from . import JSONSCHEMA_JSON, SCHEMA_DIR, TestCase


class TestCLI(TestCase):
    def test_validate_json(self):
        # self validate root jsonschema.schema.json
        json_file = os.path.join(SCHEMA_DIR, JSONSCHEMA_JSON)
        self._test_cli("validate_json", [json_file])
        # load schema from local file
        # self._test_cli("validate_json", [json_file, json_file])
