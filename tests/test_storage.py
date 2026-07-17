"""TODO"""

import datetime
import json
from pathlib import Path
import pickle
from tempfile import TemporaryDirectory
from threading import Thread
from typing import Any
from unittest import TestCase

from click.testing import CliRunner
import httpx
import pandas as pd
from pandas.testing import assert_frame_equal
import uvicorn

from datatools.__main__ import main
from datatools.exceptions import (
    StorageFileExistsError,
    StorageFileNotFoundError,
    StorageInvalidNameError,
)
from datatools.process.task import AnnotatedFunction
from datatools.storage import _infer_storage_class
from datatools.storage.base import DataStorage
from datatools.storage.cli import CliWrapperDataStorage
from datatools.storage.file import FileDataStorage, JsonFileMetadataStorage
from datatools.storage.http import HttpDataStorage, make_server_app
from datatools.storage.memory import MemoryDataStorage
from datatools.storage.sql import SqlDataStorage
from datatools.types import (
    JSON_SCHEMA_FILE_RESOURCE,
    LOCKFILE_SUFFIX,
    RDF_CONTEXT,
    SINGLE_OUTPUT_PARAM_NAME,
    TEMPFILE_SUFFIX,
    ReadableByteBuffer,
    URIRefs as u,
)
from datatools.utils import (
    DEFAULT_ENCODING,
    get_free_port,
    get_item_or_first,
    identity,
    query_sql,
    sql_query_result_to_csv,
    start_http_server,
    wait_for_url,
)
from tests.base import TempdirTestCase

QueryParameterUri = f'{u.createdBy.label}.{u.usedInput.label}[?({u.roleName.label} == "uri")].{u.value.label}'  # noqa:E501
QueryTimestamp = f"{u.createdBy.label}.{u.datetime.label}"


def _test_action_sequence(self: TestCase, storage: DataStorage):
    """TODO"""

    # insert our first data
    name1 = "data1"
    data1 = b"data1"

    storage.info()

    # prevent invalid name
    self.assertRaises(StorageInvalidNameError, storage.write, "\n" + name1, data1)

    storage.write(name1, data1)
    # now it exists
    self.assertTrue(storage.read(name1))
    # now we cannot add it again
    self.assertRaises(StorageFileExistsError, storage.write, name1, data1)
    # we can retreive it
    self.assertEqual(storage.read(name1), data1)

    name2 = "data2"
    data2 = b"data2"
    mdata2_key, mdata2_val = "metadata2_a", 10
    self.assertFalse(storage.has(name2))
    # but even though it does not exist, we can add metadata
    storage.metadata(name2).set(mdata2_key, mdata2_val)
    # and can retrieve it
    self.assertEqual(
        get_item_or_first(storage.metadata(name2).get(mdata2_key)), mdata2_val
    )
    # now we insert and retrieve data
    storage.write(name2, data2)
    self.assertEqual(storage.read(name2), data2)
    # list all names:
    self.assertEqual(set(storage.find()), {name1, name2})
    # list via iterator
    self.assertEqual(set(storage.find()), {name1, name2})

    # filter by metadata
    self.assertEqual(set(storage.find(**{mdata2_key: mdata2_val})), {name2})

    # delete
    storage.delete(name1)
    self.assertFalse(storage.has(name1))

    # try if exception is raised
    self.assertRaises(StorageFileNotFoundError, storage.read, name1)
    self.assertRaises(StorageFileNotFoundError, storage.delete, name1)

    # change/update metadata
    storage.metadata(name2).set(mdata2_key, "CHANGED")
    # and can retrieve it
    self.assertEqual(
        get_item_or_first(storage.metadata(name2).get(mdata2_key)), "CHANGED"
    )


class TestStorageMemory(TestCase):
    """TODO"""

    def test_action_sequence(self):
        """TODO"""
        storage = MemoryDataStorage()
        _test_action_sequence(self, storage)

    def test_no_convserion(self):
        """TODO"""
        storage = MemoryDataStorage()
        storage.write("data", b"data")
        # {"x": None} => use default (passthrough) for parameter x of identity
        storage.task(identity, input_converters={"x": None})("data2", "data")
        self.assertTrue(storage.has("data2"))


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

    def test_temp_and_lockfiles(self):
        """TODO"""
        storage = FileDataStorage(str(self.temp_dir))
        name = "example.txt"
        name_temp = "example.txt" + TEMPFILE_SUFFIX
        name_lock = "example.txt" + LOCKFILE_SUFFIX
        (self.temp_dir / name).touch()
        # create lock/tempfile
        (self.temp_dir / (name_lock)).touch()
        (self.temp_dir / (name_temp)).touch()

        # lock/tempfile should not be found
        self.assertEqual(list(storage.find()), [name])

        # namesshould not ba allowed
        self.assertRaises(
            StorageInvalidNameError, storage._assert_valid_name, name_temp
        )
        self.assertRaises(
            StorageInvalidNameError, storage._assert_valid_name, name_lock
        )

    def test_existing_invalid_metadata(self):
        """raise exception"""
        # create invalid json
        storage = FileDataStorage(str(self.temp_dir))
        Path(self.temp_dir / "data.metadata.json").write_bytes(b"[]")
        self.assertRaises(ValueError, storage.metadata, "data")

    def test_new_name_is_path(self):
        """TODO"""
        storage = FileDataStorage(str(self.temp_dir))
        storage.write("a/b", b"")
        # "a" is alreaedy used as path
        self.assertRaises(StorageInvalidNameError, storage.write, "a", b"")


class TestStorageSql(TestCase):
    """TODO"""

    def test_action_sequence(self):
        """TODO"""
        storage = SqlDataStorage()
        _test_action_sequence(self, storage)

    def test_does_not_exist(self):
        """bypass check from base class."""
        storage = SqlDataStorage()
        self.assertRaises(StorageFileNotFoundError, storage.read, "KEY")


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
            path_server = Path(tmpdir) / "server"
            path.write_bytes(test_data)

            port = get_free_port()
            url = f"http://localhost:{port}"
            runner = CliRunner()
            thread = Thread(
                target=runner.invoke,
                args=(
                    main,
                    [
                        "storage",
                        "-l",
                        path_server.as_posix(),
                        "-c",
                        "FileDataStorage",
                        "serve",
                        "-p",
                        str(port),
                    ],
                ),
                daemon=True,
            )
            thread.start()

            wait_for_url(url)

            # import the test file via client
            runner2 = CliRunner()
            resp = runner2.invoke(
                main,
                ["storage", "-l", url, "import", "data", path.as_uri()],
            )

            # retrieve
            resp = runner2.invoke(
                main,
                ["storage", "-l", url, "read", "data"],
            )

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

            self.assertTrue(storage._has("a/b"))
            self.assertFalse(storage._has("b"))
            self.assertRaises(Exception, storage._has, "/a")
            self.assertRaises(Exception, storage._has, "a")


class TestUseCases(TestCase):
    """TODO"""

    def test_infer_storage_class(self):
        """should fail on unknown URI"""
        self.assertRaises(
            NotImplementedError, _infer_storage_class, "xyz://bad/protocol"
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

            self.assertEqual(storage.read(name), test_data)
            # should have meta data from import action
            self.assertEqual(
                get_item_or_first(storage.metadata(name).get(QueryParameterUri)),
                uri,
            )

            # import from path
            uri = filepath.as_uri()
            name = storage.import_from_uri(uri)
            self.assertEqual(storage.read(name), test_data)
            self.assertEqual(
                get_item_or_first(storage.metadata(name).get(QueryParameterUri)),
                uri,
            )

            # import from sql
            query = "select 1 as a"
            uri = "sqlite:///:memory:"
            name = storage.import_from_uri(uri, query=query)
            self.assertEqual(storage.read(name).replace(b"\r", b""), b"a\n1\n")
            # TODO add query?
            self.assertEqual(
                get_item_or_first(storage.metadata(name).get(QueryParameterUri)),
                uri,
            )

            # check metadata
            metadata_all: dict = get_item_or_first(storage.metadata(name).get("$"))  # type:ignore - root should be dict
            del metadata_all["@context"]

            metadata_creation_event: dict = metadata_all[u.createdBy.label]

            task_uuid = metadata_creation_event[u.taskId.label]
            timestamp = metadata_creation_event[u.datetime.label]
            self.assertTrue(task_uuid, "")
            event_id = f"event:{task_uuid}/{timestamp}"

            metadata_all_expected = {
                "$schema": JSON_SCHEMA_FILE_RESOURCE,
                "@id": event_id + "/output/" + SINGLE_OUTPUT_PARAM_NAME,
                # "@type": u.FileResource.label,
                u.name.label: ":memory:",
                # file info
                u.hash.label: "sha256:309b0e45a73d3fc5325e2b6ed0a01ef8b9cde6b05a5633c1f893f970d52bfddc",  # noqa:E501
                u.bytes.label: 4,
                # file saved with info
                u.serializedWith.label: {
                    # "@type": u.Serialization.label,
                    u.usedFunction.label: {
                        "@id": "function:"
                        + AnnotatedFunction(sql_query_result_to_csv).function_id,
                        # "@type": u.Function.label,
                        "description": sql_query_result_to_csv.__doc__,
                    },
                    u.roleName.label: SINGLE_OUTPUT_PARAM_NAME,
                },
                # file generation info
                u.createdBy.label: {
                    "@id": event_id,
                    # "@type": u.CreationEvent.label,
                    # context
                    u.datetime.label: metadata_creation_event[u.datetime.label],
                    u.creator.label: metadata_creation_event[u.creator.label],
                    # Job
                    u.usedFunction.label: {
                        "@id": "function:QUERY",
                        # "@type": u.Function.label,
                        "description": query_sql.__doc__,
                    },
                    u.taskId.label: task_uuid,
                    u.usedInput.label: [
                        {
                            "@id": event_id + "/input/uri",
                            # "@type": u.LiteralParameter.label,
                            u.roleName.label: "uri",
                            u.value.label: "sqlite:///:memory:",
                        },
                        {
                            "@id": event_id + "/input/query",
                            # "@type": u.LiteralParameter.label,
                            u.roleName.label: "query",
                            u.value.label: "select 1 as a",
                        },
                        {
                            "@id": event_id + "/input/options",
                            # "@type": u.LiteralParameter.label,
                            u.roleName.label: "options",
                            # u.value.label: None, # no value
                        },
                    ],
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
            storage.write(name, pickle.dumps(3))

        task_create_output = storage.task(
            function,
            input_converters=dict.fromkeys(inputs, pickle.load),
            output_converters=dict.fromkeys(outputs, pickle.dump),
        )

        # call without output name should cause error
        self.assertRaises(Exception, task_create_output)
        # call without input name should cause error
        self.assertRaises(Exception, task_create_output, "OUTPUT")

        # try to call mutliple times - but only of output does not exist
        for _ in range(2):
            if not all(storage.has(name) for name in outputs.values()):
                task_create_output(**outputs, **inputs)

        self.assertTrue(all(storage.has(name) for name in outputs.values()))

        self.assertEqual(count_calls, 1)

        # check that metadata should also be writtem
        for name in outputs.values():
            task_timestamp_s = str(
                get_item_or_first(storage.metadata(name).get(QueryTimestamp))
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

        fid_convert = "convert1"
        fid_bytes2json = "bytes2json"

        def generate1() -> bytes:
            return data1

        @AnnotatedFunction.wrap(function_id=fid_convert)
        def convert(data: list) -> list:
            return [x + 1 for x in data]

        json_load = AnnotatedFunction(json.load, function_id=fid_bytes2json)

        # "output": None -> already bytes
        task_generate = storage.task(
            function=generate1,
            output_converters={"output": None},
            metadata_generator=lambda _: {f"{u.mediatype.label}": "application/json"},
            skip_finished=True,
        )
        task_convert = storage.task(
            function=convert,
            output_converters={"output": json.dump},  # type:ignore TODO: why?
            input_converters={"data": json_load},
            skip_finished=True,
        )

        key1 = f"generated_{task_generate.get_task_id()}.json"
        task_generate(output=key1)
        task_generate(key1)  # does nothing, because already created

        # dynamically create id for next step (use same arguments as in actuall)
        key2 = f"converted_{task_convert.get_task_id(data=key1)}.json"
        task_convert(output=key2, data=key1)

        # check metadata
        self.assertEqual(
            get_item_or_first(
                storage.metadata(key2).get(
                    f"{u.createdBy.label}.{u.usedFunction.label}.@id"
                )
            ),
            "function:" + fid_convert,
        )

        self.assertEqual(
            get_item_or_first(storage.metadata(key1).get(f"{u.mediatype.label}")),
            "application/json",
        )

    def test_run_through_rdf(self):
        """TODO"""
        data = {"@context": RDF_CONTEXT, "@id": "urn:dummy", "key": "value"}
        resp = JsonFileMetadataStorage._run_through_rdf(data)
        self.assertEqual(resp, data)

    def test_use_metadata_for_loaders(self):
        """loader/dumper functions should get their default valuesfrom metadata."""

        def loadb(buf: ReadableByteBuffer, encoding: str = DEFAULT_ENCODING) -> Any:
            return buf.read().decode(encoding=encoding)

        def dumpb(text: str) -> bytes:
            return text.encode()

        st = MemoryDataStorage()
        st.write("data", "Ünicöde".encode(encoding="windows-1252"))

        task = st.task(
            dumpb,
            input_converters=loadb,
        )

        # running task as is should fail
        self.assertRaises(UnicodeDecodeError, task, "data2", text="data")

        # but if someone writes puts encoding info in metadata, it should pick it up
        # TODO: where exactly in metadata? directly in FileResource?
        st.metadata("data").set("encoding", "windows-1252")
        # now it works
        task("data2", text="data")


class TestCache(TestCase):
    """TODO"""

    def test_cache(self):
        """TODO"""
        df = pd.DataFrame([{"a": 1, "b": "Ö", "c": 1.2}, {"a": 2, "b": "ß"}])

        # fixme also use buffer / byte iterator / string iterator
        c_pickle = MemoryDataStorage().cache(pickle.dump, pickle.load)
        # use orient="table" to preserve index names
        c_json = MemoryDataStorage().cache(
            # actually to string, but we auto convert to bytes
            lambda df, buf: df.to_json(buf, orient="table"),
            lambda buf: pd.read_json(buf, orient="table"),
        )

        # FIXME: must save index names and col dims in metadata
        # for round trip

        def read_csv(buf: ReadableByteBuffer) -> pd.DataFrame:
            df = pd.read_csv(buf, encoding=DEFAULT_ENCODING)
            df = df.set_index([c for c in df.columns if c.startswith("$")])
            df = df.rename_axis(index=[str(c)[1:] for c in df.index.names])
            return df

        c_csv = MemoryDataStorage().cache(
            lambda df, buf: df.rename_axis(
                index=[f"${c}" for c in df.index.names]
            ).to_csv(buf, encoding=DEFAULT_ENCODING),
            read_csv,
        )

        def f_no_index():
            # NOTE: cannot use "index"
            return df.rename_axis(index="$index")

        def f_single_index():
            return df.set_index("a")

        def f_multi_index():
            return df.set_index(["a", "b"])

        # test mastrix of functions and cache types
        for f in [
            f_no_index,
            f_single_index,
            f_multi_index,
        ]:
            for i, cache in enumerate(
                [
                    c_pickle,
                    c_json,
                    c_csv,  # FIXME: must save index names and col dims in metadata
                ]
            ):
                df1 = f()
                df2 = cache(f)()
                try:
                    assert_frame_equal(f(), cache(f)())
                except Exception:
                    print("=====", f.__name__, f"cache:{i}")
                    print(df1)
                    print(df2)
                    raise

        def test_cache_with_custom_name(self):
            pass
