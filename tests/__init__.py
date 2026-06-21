"""package init"""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
from unittest import TestCase

from datatools.utils import get_free_port

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
        super().setUpClass()
        # create temp dir
        cls._temp_dir = TemporaryDirectory()
        cls.temp_dir = Path(cls._temp_dir.name)
        assert cls.temp_dir.exists()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temp_dir.cleanup()
        assert not cls.temp_dir.exists()
        super().tearDownClass()
