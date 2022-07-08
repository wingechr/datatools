import os
import subprocess as sp
import tempfile

from . import TestCase


class __TODO__TestScons(TestCase):
    def test_scons(self):
        root_dir = os.path.abspath(".")
        example_dir = os.path.join(root_dir, "test", "example")
        sconstruct = os.path.join(example_dir, "SConstruct")
        with tempfile.TemporaryDirectory() as tempdir:
            cmd = [
                "scons",
                "--directory",
                tempdir,
                "--file",
                sconstruct,
                "--srcdir",
                example_dir,
            ]
            environ = os.environ.copy()
            environ["PYTHONPATH"] = root_dir
            prc = sp.Popen(cmd, shell=True, env=environ, stdout=sp.PIPE)
            _ = prc.communicate()
            self.assertEqual(prc.returncode, 0)
