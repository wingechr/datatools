"""package init"""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase


def get_item_or_first(x):
    """TODO"""
    if isinstance(x, list):
        return x[0]
    return x


class TempdirTestCase(TestCase):
    """TODO"""

    def setUp(self) -> None:
        """TODO"""
        super().setUp()
        # create temp dir
        self._temp_dir = TemporaryDirectory()
        self.temp_dir = Path(self._temp_dir.name)
        # assert cls.temp_dir.exists() # noqa

    def tearDown(self) -> None:
        """TODO"""
        self._temp_dir.cleanup()
        # assert not cls.temp_dir.exists() # noqa
        super().tearDownClass()
