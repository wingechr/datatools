import logging
import os
import tempfile
import unittest

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.INFO
)

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

TEST_HASH = {"bytes": b"TESTDATA", "file_id": "f07930dff605c976cfd981d3356136fd"}


def create_testfile(bytes):
    """Return path"""
    with tempfile.NamedTemporaryFile("wb", delete=False) as file:
        file.write(bytes)
    return file.name


class TestCase(unittest.TestCase):
    @staticmethod
    def get_data_filepath(filename):
        return os.path.abspath(os.path.join(TEST_DATA_DIR, filename))
