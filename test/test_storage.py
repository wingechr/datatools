import unittest
import logging
import tempfile
import os

from datatools.storage.files import FileSystemStorage
from datatools.storage.metadata import SqliteMetadataStorage
from datatools.storage.exceptions import ObjectNotFoundException

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
)


class TestFileSystemStorage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.TemporaryDirectory()
        cls.tempdir.__enter__()
        cls.storage = FileSystemStorage(data_dir=cls.tempdir.name)

    @classmethod
    def tearDownClass(cls):
        cls.tempdir.__exit__(None, None, None)
        pass

    def test_storage(self):
        file_name = "900150983cd24fb0d6963f7d28e17f72"

        # try to load file that has not been added
        self.assertRaises(ObjectNotFoundException, lambda: self.storage.get(file_name))

        # add file and check if id matches name
        filepath = os.path.join(TEST_DATA_DIR, file_name)
        with open(filepath, "rb") as file:
            file_id = self.storage.set(file)
        self.assertEqual(file_id, file_name)

        # load file (and add it again)
        with self.storage.get(file_id) as file:
            file_id = self.storage.set(file)
        self.assertEqual(file_id, file_name)


class TestSqliteMetadataStorage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempfile = tempfile.NamedTemporaryFile(delete=True)
        cls.tempfile.close()
        cls.db = SqliteMetadataStorage(database=cls.tempfile.name, default_user="test")

    @classmethod
    def tearDownClass(cls):
        os.remove(cls.tempfile.name)

    def test_storage(self):
        file_id = "900150983cd24fb0d6963f7d28e17f72"
        dataset_1 = {"key1": None, "key2": "text"}
        dataset_2 = {"key2": "text updated", "key3": [1, 2, 3]}
        with self.db:
            self.assertRaises(
                ObjectNotFoundException, lambda: self.db.get(file_id, "key2")
            )

            self.db.set(file_id, dataset_1)
            self.db.set(file_id, dataset_2)
            value_2 = self.db.get(file_id, "key2")

        self.assertEqual(value_2, "text updated")
