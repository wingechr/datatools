"""TODO"""

import datetime
import json
import logging
from pathlib import Path
import pickle
from tempfile import TemporaryDirectory
from threading import Thread
from unittest import TestCase

from click.testing import CliRunner
import httpx
import uvicorn

from datatools.__main__ import main
from datatools.exceptions import (
    StorageFileExistsError,
    StorageFileNotFoundError,
    StorageInvalidNameError,
)
from datatools.process.task import AnnotatedFunction
from datatools.storage.__main__ import infer_storage_class
from datatools.storage.base import DataStorage
from datatools.storage.cli import CliWrapperDataStorage
from datatools.storage.file import FileDataStorage, FileDataStorageWithRdfMetadata
from datatools.storage.http import HttpDataStorage, make_server_app
from datatools.storage.memory import MemoryDataStorage
from datatools.storage.sql import SqlDataStorage
from datatools.types import (
    PROP_CREATOR,
    PROP_DATETIME,
    PROP_FUNCTION,
    PROP_GENERATED_BY,
    PROP_HASHSUM,
    PROP_JOB,
    PROP_PARAMETER,
    PROP_PARAMETER_NAME,
    PROP_PARAMETER_VALUE,
    PROP_SAVED_WITH,
    PROP_SIZE,
    SINGLE_OUTPUT_PARAM_NAME,
)
from datatools.utils import (
    get_free_port,
    query_sql,
    sql_query_result_to_csv_bytes,
    start_http_server,
    wait_for_url,
)
from tests.base import TempdirTestCase

QueryParameterUri = f'{PROP_GENERATED_BY}.{PROP_JOB}.{PROP_PARAMETER}[?({PROP_PARAMETER_NAME} == "uri")].{PROP_PARAMETER_VALUE}'  # noqa:E501
QueryTimestamp = f"{PROP_GENERATED_BY}.{PROP_DATETIME}"


def get_item_or_first(x):
    """TODO"""
    if isinstance(x, list):
        return x[0]
    return x


def _test_action_sequence(self: TestCase, storage: DataStorage):
    """TODO"""

    # insert our first data
    name1 = "data1"
    data1 = b"data1"

    storage.info()

    # prevent invalid name
    self.assertRaises(StorageInvalidNameError, storage.__setitem__, "\n" + name1, data1)

    storage[name1] = data1
    # now it exists
    self.assertTrue(name1 in storage)
    # now we cannot add it again
    self.assertRaises(StorageFileExistsError, storage.__setitem__, name1, data1)
    # we can retreive it
    self.assertEqual(storage[name1], data1)

    name2 = "data2"
    data2 = b"data2"
    mdata2_key, mdata2_val = "metadata2_a", 10
    self.assertFalse(name2 in storage)
    # but even though it does not exist, we can add metadata
    storage.metadata(name2)[mdata2_key] = mdata2_val
    # and can retrieve it
    self.assertEqual(next(iter(storage.metadata(name2)[mdata2_key])), mdata2_val)
    # now we insert and retrieve data
    storage[name2] = data2
    self.assertEqual(storage[name2], data2)
    # list all names:
    self.assertEqual(set(storage.find()), {name1, name2})
    # list via iterator
    self.assertEqual(set(storage), {name1, name2})

    # filter by metadata
    self.assertEqual(set(storage.find(**{mdata2_key: mdata2_val})), {name2})

    # delete
    del storage[name1]
    self.assertFalse(name1 in storage)

    # try if exception is raised
    self.assertRaises(StorageFileNotFoundError, storage.__getitem__, name1)
    self.assertRaises(StorageFileNotFoundError, storage.__delitem__, name1)

    # change/update metadata
    storage.metadata(name2)[mdata2_key] = "CHANGED"
    # and can retrieve it
    self.assertEqual(next(iter(storage.metadata(name2)[mdata2_key])), "CHANGED")


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

    def test_validate_name(self):
        """name cannot be an absolute path"""
        storage = FileDataStorage(str(self.temp_dir))

        # no exception
        storage._assert_valid_name("file.txt")
        storage._assert_valid_name("folder/file.txt")

        self.assertRaises(
            StorageInvalidNameError, storage._assert_valid_name, "/root/dir"
        )
        self.assertRaises(StorageInvalidNameError, storage._assert_valid_name, "../xyz")

    def test_existing_invalid_metadata(self):
        """raise exception"""
        # create invalid json
        storage = FileDataStorage(str(self.temp_dir))
        Path(self.temp_dir / "data.metadata.json").write_bytes(b"[]")
        self.assertRaises(ValueError, storage.metadata, "data")

    def test_new_name_is_path(self):
        """TODO"""
        storage = FileDataStorage(str(self.temp_dir))
        storage["a/b"] = b""
        # "a" is alreaedy used as path
        self.assertRaises(StorageInvalidNameError, storage.__setitem__, "a", b"")


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

    def _start_server(self, remote_storage: DataStorage) -> str:
        host = "127.0.0.1"
        port = get_free_port()
        app = make_server_app(data_storage=remote_storage)

        config = uvicorn.Config(app, host=host, port=port)
        server = uvicorn.Server(config)
        thread = Thread(target=server.run, daemon=True)
        thread.start()

        url = f"http://{host}:{port}"

        wait_for_url(url)
        return url

    def test_action_sequence_w_memory_backend(self):
        """TODO"""
        remote_storage = MemoryDataStorage()
        url = self._start_server(remote_storage)
        storage = HttpDataStorage(url)
        _test_action_sequence(self, storage)

    def test_action_sequence_w_file_backend(self):
        """TODO"""
        with TemporaryDirectory() as tempdir:
            remote_storage = FileDataStorage(tempdir)
            url = self._start_server(remote_storage)
            storage = HttpDataStorage(url)
            _test_action_sequence(self, storage)

            # some additional tests (bypass base functions)
            resp = httpx.post(url + "/data")
            self.assertEqual(resp.status_code, 405)  # Method Not Allowed

            # add data
            resp = httpx.put(url + "/data/a/b", content=b"data")
            resp.raise_for_status()
            # invalid requests
            resp = httpx.put(url + "/data//a", content=b"data")
            self.assertEqual(resp.status_code, 400)

            self.assertTrue(storage._contains("a/b"))
            self.assertFalse(storage._contains("b"))
            self.assertRaises(Exception, storage._contains, "/a")
            self.assertRaises(Exception, storage._contains, "a")


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
            name = storage.import_from_uri(uri)

            self.assertEqual(storage[name], test_data)
            # should have meta data from import action
            self.assertEqual(
                get_item_or_first(storage.metadata(name)[QueryParameterUri]),
                uri,
            )

            # import from path
            uri = filepath.as_uri()
            name = storage.import_from_uri(uri)
            self.assertEqual(storage[name], test_data)
            self.assertEqual(
                get_item_or_first(storage.metadata(name)[QueryParameterUri]),
                uri,
            )

            # import from sql
            query = "select 1 as a"
            uri = "sqlite:///:memory:"
            name = storage.import_from_uri(uri, query=query)
            self.assertEqual(storage[name].replace(b"\r", b""), b"a\n1\n")
            # TODO add query?
            self.assertEqual(
                get_item_or_first(storage.metadata(name)[QueryParameterUri]),
                uri,
            )

            # check metadata
            metadata_all = get_item_or_first(storage.metadata(name)["$"])

            metadata_activity: dict = metadata_all[PROP_GENERATED_BY]  # type:ignore

            job_id = metadata_activity[PROP_JOB]["@id"]
            self.assertTrue(job_id, "")
            activity_id = (
                job_id.replace("job:", "activity:")
                + "-"
                + metadata_activity[PROP_DATETIME]
            )

            metadata_all_expected = {
                "@id": "TODO",
                "@type": "Resource",
                # file info
                PROP_SIZE: 4,
                PROP_HASHSUM: "md5:34ff2335cbe2045ddc3b78993d1e971d",
                # file saved with info
                PROP_SAVED_WITH: {
                    "@id": activity_id + "/output/" + SINGLE_OUTPUT_PARAM_NAME,
                    "@type": "Output",
                    PROP_FUNCTION: {
                        "@id": "sql_query_result_to_csv_bytes",
                        "@type": "Function",
                        "description": sql_query_result_to_csv_bytes.__doc__,
                    },
                },
                # file generation info
                PROP_GENERATED_BY: {
                    "@id": activity_id,
                    "@type": "Activity",
                    # context
                    PROP_DATETIME: metadata_activity[PROP_DATETIME],
                    PROP_CREATOR: metadata_activity[PROP_CREATOR],
                    # Job
                    PROP_JOB: {
                        "@id": job_id,
                        "@type": "Job",
                        PROP_FUNCTION: {
                            "@id": "QUERY",
                            "@type": "Function",
                            "description": query_sql.__doc__,
                        },
                        PROP_PARAMETER: [
                            {
                                "@id": activity_id + "/input/uri",
                                "@type": "Input",
                                PROP_PARAMETER_NAME: "uri",
                                PROP_PARAMETER_VALUE: "sqlite:///:memory:",
                            },
                            {
                                "@id": activity_id + "/input/query",
                                "@type": "Input",
                                PROP_PARAMETER_NAME: "query",
                                PROP_PARAMETER_VALUE: "select 1 as a",
                            },
                            {
                                "@id": activity_id + "/input/options",
                                "@type": "Input",
                                PROP_PARAMETER_NAME: "options",
                                PROP_PARAMETER_VALUE: None,
                            },
                        ],
                    },
                },
            }
            self.maxDiff = None
            self.assertEqual(metadata_all_expected, metadata_all, metadata_all)

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

    def test_use_case_task_graph(self):
        """TODO

        Cache is not useful in a large graph of operations,
        becasue we still have to do all the dump/load operations.

        In snakemake, we set rules that need either a script or a function
        that take output and input arguments and no return (Scons is pretty similar)
        plus arguments for those.
        the build tool decides if and in what orderto call the functons with the
        arguments.

        task(output:Path, input1:Path, input2:Path, param3:int=default) -> None



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
        for name in inputs.values():
            storage[name] = pickle.dumps(3)

        task_create_output = storage.task(
            function,
            input_converters=dict.fromkeys(inputs, pickle.loads),
            output_converters=dict.fromkeys(outputs, pickle.dumps),
        )

        # try to call mutliple times - but only of output does not exist
        for _ in range(2):
            if not all(name in storage for name in outputs.values()):
                task_create_output(**outputs, **inputs)

        self.assertTrue(all(name in storage for name in outputs.values()))

        self.assertEqual(count_calls, 1)

        # check that metadata should also be writtem
        for name in outputs.values():
            task_timestamp_s = str(
                get_item_or_first(storage.metadata(name)[QueryTimestamp])
            )
            datetime.datetime.fromisoformat(task_timestamp_s)

    def test_use_chain_of_tasks_w_storage(self):
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

        @AnnotatedFunction.wrap(function_id=fid_convert)
        def convert(data: list) -> list:
            return [x + 1 for x in data]

        loads = AnnotatedFunction(json.loads, function_id=fid_bytes2json)

        # "output": None -> already bytes
        task_generate = storage.task(generate1, {"output": None}, skip_finished=True)
        task_convert = storage.task(
            convert,
            {"output": lambda x: json.dumps(x).encode()},
            {"data": loads},
            skip_finished=True,
        )

        key1 = f"generated_{task_generate.get_job_hashsum()}.json"
        task_generate(output=key1)
        task_generate(key1)  # does nothing, because already created

        # dynamically create id for next step (use same arguments as in actuall)
        key2 = f"converted_{task_convert.get_job_hashsum(data=key1)}.json"
        task_convert(output=key2, data=key1)

        # check metadata
        self.assertEqual(
            get_item_or_first(
                storage.metadata(key2)[
                    f"{PROP_GENERATED_BY}.{PROP_JOB}.{PROP_FUNCTION}.@id"
                ]
            ),
            fid_convert,
        )
