import os

from datatools.test import TestCase
from datatools.utils.temp import NamedClosedTemporaryFile


class TestCLI(TestCase):
    def test_download(self):
        with NamedClosedTemporaryFile() as filepath:
            self.assertTrue(os.path.getsize(filepath) == 0)
            self._test_cli("load", ["http://example.com", filepath, "--overwrite"])
            self.assertTrue(os.path.getsize(filepath) > 0)
