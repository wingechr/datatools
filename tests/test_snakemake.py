"""TODO"""

import os
from pathlib import Path
import subprocess

from datatools.storage.file import FileDataStorage
from tests.base import TempdirTestCase


class TestSnakemake(TempdirTestCase):
    """TODO"""

    def test_snakemake(self):
        """TODO"""
        data_storage = FileDataStorage(str(self.temp_dir))
        self.assertFalse("converted.json" in data_storage)

        snakefile = Path(__file__).parent / "test_snakemake.Snakefile"
        env = os.environ.copy()
        env["PYTHONPATH"] = str(snakefile.parent.parent.resolve())
        subprocess.run(
            ["snakemake", "-j", "1", "-s", str(snakefile)],  # noqa:S607
            check=True,
            env=env,
            cwd=self.temp_dir,
        )

        self.assertTrue("converted.json" in data_storage)
