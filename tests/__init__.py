"""package init"""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
    force=True,
)


class TempdirTestCase(TestCase):
    """TODO"""

    @classmethod
    def setUpClass(cls) -> None:
        """TODO"""
        super().setUpClass()
        # create temp dir
        cls._temp_dir = TemporaryDirectory()
        cls.temp_dir = Path(cls._temp_dir.name)
        # assert cls.temp_dir.exists() # noqa

    @classmethod
    def tearDownClass(cls) -> None:
        """TODO"""
        cls._temp_dir.cleanup()
        # assert not cls.temp_dir.exists() # noqa
        super().tearDownClass()
