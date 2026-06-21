from threading import Thread
from unittest import TestCase

import uvicorn

from datatools.storage.classes import (
    FileDataStorage,
    HttpDataStorage,
    MemoryDataStorage,
)
from datatools.storage.server import make_server_app
from datatools.storage.types import DataStorage, StorageFileExistsError
from datatools.utils import get_free_port

from . import TempdirTestCase


class AbstractTestActionMixin(TestCase):
    storage: DataStorage


class TestActionMixin:
    """TODO"""

    def test_action_sequence(self: AbstractTestActionMixin):
        """TODO"""

        # insert our first data
        uid1 = "data1"
        data1 = b"data1"

        self.storage[uid1] = data1
        # now it exists
        self.assertTrue(uid1 in self.storage)
        # now we cannot add it again
        self.assertRaises(StorageFileExistsError, self.storage.__setitem__, uid1, data1)
        # we can retreive it
        self.assertEqual(self.storage[uid1], data1)

        uid2 = "data2"
        data2 = b"data2"
        mdata2_key, mdata2_val = "metadata2_a", "10"
        self.assertFalse(uid2 in self.storage)
        # but even though it does not exist, we can add metadata
        self.storage.metadata(uid2)[mdata2_key] = mdata2_val
        # and can retrieve it
        self.assertEqual(
            next(iter(self.storage.metadata(uid2)[mdata2_key])), mdata2_val
        )
        # now we insert and retrieve data
        self.storage[uid2] = data2
        self.assertEqual(self.storage[uid2], data2)
        # list all uids:
        self.assertEqual(set(self.storage.list()), {uid1, uid2})
        # filter by metadata
        self.assertEqual(set(self.storage.list(**{mdata2_key: mdata2_val})), {uid2})

        # delete
        del self.storage[uid1]
        self.assertFalse(uid1 in self.storage)


class TestStorageMemory(TestActionMixin, TestCase):
    """TODO"""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.storage = MemoryDataStorage()


class TestStorageFiles(TestActionMixin, TempdirTestCase):
    """TODO"""

    @classmethod
    def setUpClass(cls):
        """TODO"""
        super().setUpClass()
        cls.storage = FileDataStorage(cls.temp_dir)


class TestStoragehttpServer(TestActionMixin, TestCase):
    """TODO"""

    @classmethod
    def setUpClass(cls) -> None:
        """TODO"""
        super().setUpClass()
        # create temp dir

        remote_storage = MemoryDataStorage()
        host = "127.0.0.1"
        port = get_free_port()
        app = make_server_app(data_storage=remote_storage)
        server_thread = Thread(
            target=uvicorn.run,
            kwargs={"app": app, "host": host, "port": port},
            daemon=True,
        )
        server_thread.start()
        cls.storage = HttpDataStorage(f"http://{host}:{port}")
