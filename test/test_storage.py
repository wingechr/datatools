import unittest
import logging
import tempfile
import os

from datatools.storage.combined import CombinedLocalStorage
from datatools.storage.exceptions import ObjectNotFoundException
from datatools.utils import get_data_hash, json_loadb
from datatools.package import Package, DataResource, PathResource
from datatools.exceptions import ValidationException

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
)


class TmpCombinedStorage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.TemporaryDirectory()
        cls.tempdir.__enter__()
        cls.storage = CombinedLocalStorage(data_dir=cls.tempdir.name)
        cls.storage.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.storage.__exit__(None, None, None)
        cls.tempdir.__exit__(None, None, None)


class TestFileSystemStorage(TmpCombinedStorage):
    def test_storage(self):
        file_name = "900150983cd24fb0d6963f7d28e17f72"

        # try to load file that has not been added
        self.assertRaises(
            ObjectNotFoundException, lambda: self.storage.files.get(file_name)
        )

        # add file and check if id matches name
        filepath = os.path.join(TEST_DATA_DIR, file_name)
        with open(filepath, "rb") as file:
            file_id = self.storage.files.set(file)
        self.assertEqual(file_id, file_name)

        # load file (and add it again)
        with self.storage.files.get(file_id, check_integrity=True) as file:
            file_id = self.storage.files.set(file)

        self.assertEqual(file_id, file_name)
        self.assertEqual(file_id, file.get_current_hash())


class TestSqliteMetadataStorage(CombinedLocalStorage):
    def test_storage(self):
        file_id_1 = "900150983cd24fb0d6963f7d28e17f72"
        file_id_2 = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        dataset_1 = {"key1": 100, "key2": "text", "key4": {}}
        dataset_2 = {"key1": None, "key2": "text updated", "key3": [1, 2, 3]}

        self.assertRaises(
            ObjectNotFoundException,
            lambda: self.storage.metadata.get(file_id_1, "key2"),
        )

        self.storage.metadata.set(file_id_1, dataset_1)
        self.storage.metadata.set(file_id_2, dataset_1)
        self.storage.metadata.set(file_id_1, dataset_2)
        value_2 = self.storage.metadata.get(file_id_1, "key2")
        values_all = self.storage.metadata.get_all(file_id_1)

        self.assertEqual(value_2, "text updated")
        self.assertEqual(
            values_all,
            {"key3": [1, 2, 3], "key2": "text updated", "key1": None, "key4": {}},
        )


class TestUtils(unittest.TestCase):
    def test_get_data_hash(self):
        self.assertEqual(get_data_hash(None), "37a6259cc0c1dae299a7866489dff0bd")


class TestPackage(unittest.TestCase):
    def test_package(self):
        self.assertRaises(ValidationException, lambda: DataResource(None, None))
        # no duplicate names
        self.assertRaises(
            ValidationException,
            lambda: Package(
                "p", [DataResource("r1", "data1"), DataResource("r1", "data2")]
            ),
        )

        pkg = Package("p", [DataResource("r1", "data1")])

        self.assertEqual(get_data_hash(pkg), "b7e09943103ddef777febad39eb29e17")


class TestPackageStorage(CombinedLocalStorage):
    def test_store_package(self):
        pkg = Package(
            "p",
            [DataResource("r1", "data1"), Package("p2", [PathResource("r2", "path2")])],
        )
        file_id = self.storage.files.set(pkg.__file__())
        self.assertEqual(file_id, "f164ccea8cfd020dd8c6b2b9db630c64")
        data_bytes = self.storage.files.get(file_id, check_integrity=True).read()
        data = json_loadb(data_bytes)
        pkg = Package.from_json(data)
        self.assertEqual(get_data_hash(pkg), file_id)
