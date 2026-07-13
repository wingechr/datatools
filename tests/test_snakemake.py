"""TODO"""

import os
from pathlib import Path
import subprocess
import sys
import unittest

import pytest

from datatools.storage.file import FileDataStorage
from tests.base import TempdirTestCase


@unittest.skipIf(
    sys.version_info < (3, 11),
    "Requires Python 3.11+",
)
@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="Requires Python 3.11+",
)
class TestSnakemake(TempdirTestCase):
    """TODO"""

    def test_snakemake(self):
        """TODO"""
        data_storage = FileDataStorage(str(self.temp_dir))
        self.assertFalse(data_storage.has("converted.json"))

        snakefile = Path(__file__).parent / "test_snakemake.Snakefile"
        env = os.environ.copy()
        env["PYTHONPATH"] = str(snakefile.parent.parent.resolve())
        subprocess.run(
            ["snakemake", "-j", "1", "-s", str(snakefile)],  # noqa:S607
            check=True,
            env=env,
            cwd=self.temp_dir,
        )

        self.assertTrue(data_storage.has("converted.json"))
