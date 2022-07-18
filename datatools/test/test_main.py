import os

from datatools.test import JSONSCHEMA_JSON, SCHEMA_DIR, TestCase
from datatools.utils.temp import NamedClosedTemporaryFile


class TestCLI(TestCase):
    def test_validate_json(self):
        # self validate root jsonschema.schema.json
        json_file = os.path.join(SCHEMA_DIR, JSONSCHEMA_JSON)
        self._test_cli("validate_jsonschema", [json_file])

    def test_download(self):
        with NamedClosedTemporaryFile() as filepath:
            self.assertTrue(os.path.getsize(filepath) == 0)
            self._test_cli("download", ["http://example.com", filepath, "--overwrite"])
            self.assertTrue(os.path.getsize(filepath) > 0)
