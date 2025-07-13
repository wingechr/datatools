import logging
import os
import unittest
from tempfile import TemporaryDirectory

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


class TestDatatoolsTempdir(unittest.TestCase):
    def setUp(self):
        self.tempdir = TemporaryDirectory()
        assert os.path.exists(self.tempdir.name)

    def tearDown(self):
        self.tempdir.cleanup()
        assert not os.path.exists(self.tempdir.name)
