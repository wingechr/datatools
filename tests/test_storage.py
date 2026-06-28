"""TODO"""

import logging
from tempfile import TemporaryDirectory
from threading import Thread
import time
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
from datatools.types import (
    StorageFileExistsError,
    StorageFileNotFoundError,
    StorageInvalidUidError,
)
from datatools.utils import get_free_port, get_now_str
from tests import TempdirTestCase


def _test_action_sequence_metadata(self: TestCase, storage: DataStorage):
    metadata = storage.metadata("test")

    uri = "http://example.com"
    # describe file origin
    metadata["origin"] = {
        "function": {"name": "download"},
        "parameters": {"uri": {"value": uri}},
        "timestamp": get_now_str(),
    }
    values = list(metadata["origin.parameters.uri.value"])
    logging.info(values)
    self.assertEqual(values[0], uri)


def _test_action_sequence(self: TestCase, storage: DataStorage):
    """TODO"""

    # insert our first data
    uid1 = "data1"
    data1 = b"data1"

    storage.info()

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
    self.assertEqual(set(storage.find()), {uid1, uid2})
    # filter by metadata
    self.assertEqual(set(storage.find(**{mdata2_key: mdata2_val})), {uid2})

    # delete
    del storage[uid1]
    self.assertFalse(uid1 in storage)

    # try if exception is raised
    self.assertRaises(StorageFileNotFoundError, storage.__delitem__, uid1)

    # additional tests
    _test_action_sequence_metadata(self, storage)


class TestStorageMemory(TestCase):
    """TODO"""

    def test_action_sequence(self):
        """TODO"""
        storage = MemoryDataStorage()
        _test_action_sequence(self, storage)


class TestStorageFiles(TempdirTestCase):
    """TODO"""

    def test_action_sequence(self):
        """TODO"""
        storage = FileDataStorage(str(self.temp_dir))
        _test_action_sequence(self, storage)

    def test_validate_uid(self):
        """uid cannot be an absolute path"""
        storage = FileDataStorage(str(self.temp_dir))

        # no exception
        storage._assert_valid_uid("file.txt")
        storage._assert_valid_uid("folder/file.txt")

        self.assertRaises(
            StorageInvalidUidError, storage._assert_valid_uid, "/root/dir"
        )
        self.assertRaises(StorageInvalidUidError, storage._assert_valid_uid, "../xyz")


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


class TestStorageHttpServer(TestCase):
    """TODO"""

    def test_action_sequence(self):
        """TODO"""
        remote_storage = MemoryDataStorage()
        host = "127.0.0.1"
        port = get_free_port()
        app = make_server_app(data_storage=remote_storage)

        config = uvicorn.Config(app, host=host, port=port)
        server = uvicorn.Server(config)
        thread = Thread(target=server.run, daemon=True)
        thread.start()

        while not server.started:
            time.sleep(0.01)

        storage = HttpDataStorage(f"http://{host}:{port}")
        _test_action_sequence(self, storage)
