"""TODO"""

import datetime
import json
import logging
from pathlib import Path
import pickle
from tempfile import TemporaryDirectory
from threading import Thread
import time
from unittest import TestCase

from click.testing import CliRunner
import httpx
import uvicorn

from datatools.__main__ import main
from datatools.exceptions import (
    StorageFileExistsError,
    StorageFileNotFoundError,
    StorageInvalidUidError,
)
from datatools.job.job import FunctionWrapper
from datatools.storage.__main__ import infer_storage_class
from datatools.storage.base import DataStorage
from datatools.storage.cli import CliWrapperDataStorage
from datatools.storage.file import FileDataStorage, FileDataStorageWithRdfMetadata
from datatools.storage.http import HttpDataStorage, make_server_app
from datatools.storage.memory import MemoryDataStorage
from datatools.storage.sql import SqlDataStorage
from datatools.utils import get_free_port, get_now_str, start_http_server, wait_for_url
from tests.base import TempdirTestCase


def get_item_or_first(x):
    """TODO"""
    if isinstance(x, list):
        return x[0]
    return x


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

    # prevent invalid uid
    self.assertRaises(StorageInvalidUidError, storage.__setitem__, "\n" + uid1, data1)

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
    # list via iterator
    self.assertEqual(set(storage), {uid1, uid2})

    # filter by metadata
    self.assertEqual(set(storage.find(**{mdata2_key: mdata2_val})), {uid2})

    # delete
    del storage[uid1]
    self.assertFalse(uid1 in storage)

    # try if exception is raised
    self.assertRaises(StorageFileNotFoundError, storage.__getitem__, uid1)
    self.assertRaises(StorageFileNotFoundError, storage.__delitem__, uid1)

    # change/update metadata
    storage.metadata(uid2)[mdata2_key] = "CHANGED"
    # and can retrieve it
    self.assertEqual(next(iter(storage.metadata(uid2)[mdata2_key])), "CHANGED")

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

    def test_existing_invalid_metadata(self):
        """raise exception"""
        # create invalid json
        storage = FileDataStorage(str(self.temp_dir))
        Path(self.temp_dir / "data.metadata.json").write_bytes(b"[]")
        self.assertRaises(ValueError, storage.metadata, "data")

    def test_new_uid_is_path(self):
        """TODO"""
        storage = FileDataStorage(str(self.temp_dir))
        storage["a/b"] = b""
        # "a" is alreaedy used as path
        self.assertRaises(StorageInvalidUidError, storage.__setitem__, "a", b"")


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

    def test_does_not_exist(self):
        """bypass check from base class."""
        storage = SqlDataStorage()
        self.assertRaises(StorageFileNotFoundError, storage._getitem, "KEY")


class TestCliWrapperDataStorage(TestCase):
    """TODO"""

    def test_action_sequence(self):
        """TODO"""
        with TemporaryDirectory() as tmpdir:
            storage = CliWrapperDataStorage(location=tmpdir)
            _test_action_sequence(self, storage)

    def test_cli_server(self):
        """Start server from cli"""
        with TemporaryDirectory() as tmpdir:
            # test file
            test_data = b"test"
            path = Path(tmpdir) / "data"
            path.write_bytes(test_data)

            port = get_free_port()
            url = f"http://localhost:{port}"
            runner = CliRunner()
            # serve memory storage
            thread = Thread(
                target=runner.invoke,
                args=(
                    main,
                    ["storage", "-c", "MemoryDataStorage", "serve", "-p", str(port)],
                ),
                daemon=True,
            )
            thread.start()

            wait_for_url(url)

            # import the test file via client
            runner2 = CliRunner()
            resp = runner2.invoke(
                main,
                ["storage", "-l", url, "import", str(path), "data"],
            )
            logging.error((resp.return_value, resp.stdout_bytes, resp.stderr_bytes))

            # retrieve
            resp = runner2.invoke(
                main,
                ["storage", "-l", url, "get", "data"],
            )
            logging.error((resp.return_value, resp.stdout_bytes, resp.stderr_bytes))

            self.assertEqual(resp.stdout_bytes, test_data)


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

        url = f"http://{host}:{port}"
        storage = HttpDataStorage(url)

        _test_action_sequence(self, storage)

        # some additional tests (bypass base functions)
        resp = httpx.post(url)
        self.assertEqual(resp.status_code, 405)  # Method Not Allowed


class TestUseCases(TestCase):
    """TODO"""

    def test_infer_storage_class(self):
        """should fail on unknown URI"""
        self.assertRaises(
            NotImplementedError, infer_storage_class, "xyz://bad/protocol"
        )

    def test_use_case_import_data(self):
        """TODO"""

        test_data = "äöü".encode("iso-8859-1")
        filename = "data.txt"
        storage = MemoryDataStorage()

        with TemporaryDirectory() as tmpdir:
            # create test file
            filepath = Path(tmpdir) / filename
            filepath.write_bytes(test_data)
            base_url = start_http_server(tmpdir)

            # import from http source
            uri = base_url + "/" + filename
            uid = storage.import_from_uri(uri)

            self.assertEqual(storage[uid], test_data)
            # should have meta data from import action
            self.assertEqual(
                get_item_or_first(storage.metadata(uid)["origin.parameter.uri"]),
                uri,
            )

            # import from path
            uri = filepath.as_uri()
            uid = storage.import_from_uri(uri)
            self.assertEqual(storage[uid], test_data)
            self.assertEqual(
                get_item_or_first(storage.metadata(uid)["origin.parameter.uri"]),
                uri,
            )

            # import from sql
            query = "select 1 as a"
            uri = "sqlite:///:memory:"
            uid = storage.import_from_uri(uri, query=query)
            self.assertEqual(storage[uid].replace(b"\r", b""), b"a\n1\n")
            # TODO add query?
            self.assertEqual(
                get_item_or_first(storage.metadata(uid)["origin.parameter.uri"]),
                uri,
            )

    def test_use_case_cache(self):
        """TODO"""

        storage = MemoryDataStorage()
        global count_calls
        count_calls = 0

        @storage.cache()
        def fun(x, y=3):
            global count_calls
            count_calls += 1

            return x + y

        self.assertEqual(fun(1, 2), fun(1, 2))
        self.assertEqual(count_calls, 1)
        self.assertEqual(fun(1, y=2), fun(1, y=2))
        self.assertEqual(count_calls, 1)

    def test_use_case_job_graph(self):
        """TODO

        Cache is not useful in a large graph of operations,
        becasue we still have to do all the dump/load operations.

        In snakemake, we set rules that need either a script or a function
        that take output and input arguments and no return (Scons is pretty similar)
        plus arguments for those.
        the build tool decides if and in what orderto call the functons with the
        arguments.

        job(output:Path, input1:Path, input2:Path, param3:int=default) -> None



        """
        storage = MemoryDataStorage()

        global count_calls
        count_calls = 0

        def function(param_input1, param_input2=-1):
            global count_calls
            count_calls += 1
            return param_input1 + param_input2

        outputs = {"output": "output.pickle"}
        inputs = {"param_input1": "input.pickle"}

        # generate inputs
        for uid in inputs.values():
            storage[uid] = pickle.dumps(3)

        job_create_output = storage.job(
            function,
            input_converters=dict.fromkeys(inputs, pickle.loads),
            output_converters=dict.fromkeys(outputs, pickle.dumps),
        )

        # try to call mutliple times - but only of output does not exist
        for _ in range(2):
            if not all(uid in storage for uid in outputs.values()):
                job_create_output(**outputs, **inputs)

        self.assertTrue(all(uid in storage for uid in outputs.values()))

        self.assertEqual(count_calls, 1)

        # check that metadata should also be writtem
        for uid in outputs.values():
            job_timestamp_s = str(
                get_item_or_first(storage.metadata(uid)["origin.timestamp"])
            )
            datetime.datetime.fromisoformat(job_timestamp_s)

    def test_use_chain_of_jobs_w_storage(self):
        """TODO

        In build tools like snakemake, we need to know node ids in advance.
        Using cache, we still have to dump/load every step
        So we create a sequenceof steps, with middle node ids generated
        dynamically with hashsums

        """
        storage = MemoryDataStorage()

        data1 = b"[1, 2]"

        fid_convert = "function://convert1"
        fid_bytes2json = "bytes2json"

        def generate1() -> bytes:
            return data1

        @FunctionWrapper.wrap(function_id=fid_convert)
        def convert(data: list) -> list:
            return [x + 1 for x in data]

        loads = FunctionWrapper(json.loads, function_id=fid_bytes2json)

        # "output": None -> already bytes
        job_generate = storage.job(generate1, {"output": None}, skip_finished=True)
        job_convert = storage.job(
            convert,
            {"output": lambda x: json.dumps(x).encode()},
            {"data": loads},
            skip_finished=True,
        )

        key1 = f"generated_{job_generate.get_job_hashsum()}.json"
        job_generate(output=key1)
        job_generate(key1)  # does nothing, because already created

        # dynamically create id for next step (use same arguments as in actuall)
        key2 = f"converted_{job_convert.get_job_hashsum(data=key1)}.json"
        job_convert(output=key2, data=key1)

        # check metadata
        self.assertEqual(
            get_item_or_first(storage.metadata(key1)["origin.conversion.@id"]),
            "identity",  # nothings
        )
        self.assertEqual(
            get_item_or_first(storage.metadata(key2)["origin.function.@id"]),
            fid_convert,
        )
        self.assertEqual(
            get_item_or_first(storage.metadata(key2)["origin.parameter.data.@id"]),
            fid_bytes2json,
        )
