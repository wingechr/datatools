# coding: utf-8
import json
import logging
import unittest
from tempfile import TemporaryDirectory

from datatools import Datatools
from datatools.classes import HASHED_DATA_PATH_PREFIX
from datatools.exceptions import DataDoesNotExists, DataExists, InvalidPath
from datatools.utils import normalize_path

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
)


def objects_euqal(left, right):
    left = json.dumps(left, sort_keys=True)
    right = json.dumps(right, sort_keys=True)
    return left == right


class TestUtils(unittest.TestCase):
    def test_normalize_data_path(self):
        for p, exp_np in [
            ("/my/path//", "my/path"),
            ("Lower  Case with SPACE ! ", "lower_case_with_space"),
            (
                "François fährt Straßenbahn zum Café Málaga",
                "francois_faehrt_strassenbahn_zum_cafe_malaga",
            ),
        ]:
            np = normalize_path(p)
            self.assertEqual(exp_np, np)
            # also: normalized path should always normalize to self
            np = normalize_path(exp_np)
            self.assertEqual(exp_np, np)


class TestDatatools(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = TemporaryDirectory()
        self.dt = Datatools(location=self.tempdir.__enter__())

    def tearDown(self) -> None:
        self.tempdir.__exit__(None, None, None)

    def test_local_instance(self):
        # create local instance in temporary dir
        data = b"hello world"
        data_path_user = "/My/path"
        invalid_path = HASHED_DATA_PATH_PREFIX + "my/path"

        # cannot save save data to hash subdir
        self.assertRaises(
            InvalidPath, self.dt.data_put, data=data, data_path=invalid_path
        )
        # save data without path
        data_path = self.dt.data_put(data=data)
        self.assertTrue(data_path.startswith(HASHED_DATA_PATH_PREFIX))

        # save data
        data_path = self.dt.data_put(data=data, data_path=data_path_user)
        self.assertEqual(normalize_path(data_path_user), data_path)
        # save again will fail
        self.assertRaises(
            DataExists, self.dt.data_put, data=data, data_path=data_path_user
        )
        # read it
        res = self.dt.data_get(data_path=data_path_user)
        self.assertEqual(data, res)
        # delete it ...
        self.dt.data_delete(data_path=data_path_user)
        # ... and deleting again does NOT raise an error ...
        self.dt.data_delete(data_path=data_path_user)
        # reading now will raise error
        self.assertRaises(DataDoesNotExists, self.dt.data_get, data_path=data_path_user)
        # ... and now we can save it again
        self.dt.data_put(data=data, data_path=data_path_user)

        # metadata can be saved independent of data
        metadata = {"a": [1, 2, 3], "b.c[0]": "test"}
        self.dt.metadata_put(data_path=data_path_user, metadata=metadata)

        # partial update
        metadata = {"b.c[1]": "test2"}
        self.dt.metadata_put(data_path=data_path_user, metadata=metadata)

        # get all metadata (in a list)
        metadata2 = self.dt.metadata_get(data_path=data_path_user)
        self.assertTrue(
            objects_euqal(metadata2[0], {"a": [1, 2, 3], "b": {"c": ["test", "test2"]}})
        )

        # get partial
        metadata2 = self.dt.metadata_get(data_path=data_path_user, metadata_path="b.c")
        self.assertTrue(objects_euqal(metadata2[0], ["test", "test2"]))


# class TestDatatoolsRemote(TestDatatools):
#    def setUp(self) -> None:
#        # self.server = TODO
#        self.dt = Datatools(location="http://localhost:8000").__enter__()#

#    def tearDown(self) -> None:
#        self.dt.__exit__(None, None, None)
#        # self.server.__exit__(None, None, None)
