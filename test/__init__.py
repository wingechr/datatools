import logging
import os
import unittest
import logging
import os


logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
)

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


class TestCase(unittest.TestCase):
    @staticmethod
    def get_data_filepath(filename):
        return os.path.abspath(os.path.join(TEST_DATA_DIR, filename))
