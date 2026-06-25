"""TODO"""

from tempfile import TemporaryDirectory
from threading import Thread
from unittest import TestCase

import uvicorn

from datatools.storage.classes import (
    CliWrapperDataStorage,
    DataStorage,
    FileDataStorage,
    FileDataStorageWithRdfMetadata,
    HttpDataStorage,
    MemoryDataStorage,
    SqlDataStorage,
)
from datatools.storage.server import make_server_app
from datatools.types import StorageFileExistsError
from datatools.utils import get_free_port


def _test_action_sequence(self: TestCase, storage: DataStorage):
    """TODO"""

    # insert our first data
    uid1 = "data1"
    data1 = b"data1"

    storage[uid1] = data1
    # now it exists
    self.assertTrue(uid1 in storage)
    # now we cannot add it again
    self.assertRaises(StorageFileExistsError, storage.__setitem__, uid1, data1)
    # we can retreive it
    self.assertEqual(storage[uid1], data1)

    uid2 = "data2"
    data2 = b"data2"
    mdata2_key, mdata2_val = "metadata2_a", 10
    self.assertFalse(uid2 in storage)
    # but even though it does not exist, we can add metadata
    storage.metadata(uid2)[mdata2_key] = mdata2_val
    # and can retrieve it
    self.assertEqual(next(iter(storage.metadata(uid2)[mdata2_key])), mdata2_val)
    # now we insert and retrieve data
    storage[uid2] = data2
    self.assertEqual(storage[uid2], data2)
    # list all uids:
    self.assertEqual(set(storage.list()), {uid1, uid2})
    # filter by metadata
    self.assertEqual(set(storage.list(**{mdata2_key: mdata2_val})), {uid2})

    # delete
    del storage[uid1]
    self.assertFalse(uid1 in storage)


class TestStorageMemory(TestCase):
    """TODO"""

    def test_action_sequence(self):
        """TODO"""
        storage = MemoryDataStorage()
        _test_action_sequence(self, storage)


class TestStorageFiles(TestCase):
    """TODO"""

    def test_action_sequence(self):
        """TODO"""
        with TemporaryDirectory() as tmpdir:
            storage = FileDataStorage(tmpdir)
            _test_action_sequence(self, storage)


class TestStorageFilesWithRdfMetadata(TestCase):
    """TODO"""

    def test_action_sequence(self):
        """TODO"""
        with TemporaryDirectory() as tmpdir:
            storage = FileDataStorageWithRdfMetadata(tmpdir)
            _test_action_sequence(self, storage)


class TestStorageSql(TestCase):
    """TODO"""

    def test_action_sequence(self):
        """TODO"""
        storage = SqlDataStorage()
        _test_action_sequence(self, storage)


class TestCliWrapperDataStorage(TestCase):
    """TODO"""

    def test_action_sequence(self):
        """TODO"""
        with TemporaryDirectory() as tmpdir:
            storage = CliWrapperDataStorage(location=tmpdir)
            _test_action_sequence(self, storage)


class TestStoragehttpServer(TestCase):
    """TODO"""

    def test_action_sequence(self):
        """TODO"""
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
        storage = HttpDataStorage(f"http://{host}:{port}")
        _test_action_sequence(self, storage)
