import os
import tempfile

from click.testing import CliRunner

from datatools.cli import main
from datatools.utils import make_file_writable

from . import TEST_HASH, TestCase, create_testfile


class TmpFolder(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.TemporaryDirectory()
        cls.tempdir.__enter__()
        cls.runner = CliRunner(mix_stderr=False)

    @classmethod
    def tearDownClass(cls):
        # make file writable again, so we can delete them
        for rt, ds, fs in os.walk(cls.tempdir.name):
            for f in fs:
                path = os.path.join(rt, f)
                make_file_writable(path)
        cls.tempdir.__exit__(None, None, None)


class TestCli(TmpFolder):
    def test_cli(self):
        test_data = TEST_HASH["bytes"]
        file_id = TEST_HASH["file_id"]
        filepath = create_testfile(test_data)

        res = self.runner.invoke(
            main, ["file", "-d", self.tempdir.name, "set", "-f", filepath]
        )
        file_id_new = res.stdout_bytes.decode().strip()

        self.assertEqual(res.exit_code, 0)
        self.assertEqual(file_id, file_id_new)

        # add via stdin
        with open(filepath, "rb") as file:
            bytes = file.read()
        res = self.runner.invoke(
            main, ["file", "-d", self.tempdir.name, "set"], input=bytes
        )
        file_id_new = res.stdout_bytes.decode().strip()
        self.assertEqual(res.exit_code, 0)
        self.assertEqual(file_id, file_id_new)

        res = self.runner.invoke(
            main, ["file", "-d", self.tempdir.name, "get", file_id]
        )
        bytes_read = res.stdout_bytes

        self.assertEqual(res.exit_code, 0)
        self.assertEqual(bytes, bytes_read)
