"""TODO"""

import datetime
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
from datatools.storage.file import FileDataStorage
from datatools.storage.http import HttpDataStorage, make_server_app
from datatools.storage.memory import MemoryDataStorage
from datatools.storage.sql import SqlDataStorage
from datatools.types import (
    SINGLE_OUTPUT_PARAM_NAME,
    RdfClasses as clss,
    RdfProperties as props,
)
from datatools.utils import (
    get_free_port,
    json_dumpb,
    json_loadb,
    query_sql,
    sql_query_result_to_csv_bytes,
    start_http_server,
    wait_for_url,
)
from tests.base import TempdirTestCase, get_item_or_first

QueryParameterUri = f'{props.GENERATED_BY.name}.{props.PARAMETER.name}[?({props.NAME_TITLE.name} == "uri")].{props.PARAMETER_VALUE.name}'  # noqa:E501
QueryTimestamp = f"{props.GENERATED_BY.name}.{props.DATETIME.name}"


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
        self.assertRaises(StorageFileNotFoundError, storage._read, "KEY")


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
            metadata_all: dict = get_item_or_first(storage.metadata(name).get("$"))  # type:ignore
            del metadata_all["@context"]

            metadata_activity: dict = metadata_all[props.GENERATED_BY.name]  # type:ignore

            job_id = metadata_activity[props.IDENTIFIER.name]
            self.assertTrue(job_id, "")
            activity_id = (
                job_id.replace("job:", "activity:")
                + "-"
                + metadata_activity[props.DATETIME.name]
            )

            metadata_all_expected = {
                "$schema": "TODO",
                "@id": activity_id + "/output/" + SINGLE_OUTPUT_PARAM_NAME,
                "@type": clss.OUTPUT_FILE.prefix_name,
                "identifier": ":memory:",
                # file info
                props.FILE.name: {
                    "@id": "urn:sha256:34ff2335cbe2045ddc3b78993d1e971d",
                    "@type": clss.FILE.prefix_name,
                    props.SIZE.name: 4,
                    # file saved with info
                    props.SAVED_WITH.name: {
                        props.FUNCTION.name: {
                            "@id": "sql_query_result_to_csv_bytes",
                            "@type": clss.FUNCTION.prefix_name,
                            "description": sql_query_result_to_csv_bytes.__doc__,
                        },
                        props.NAME_TITLE.name: SINGLE_OUTPUT_PARAM_NAME,
                    },
                },
                # file generation info
                props.GENERATED_BY.name: {
                    "@id": activity_id,
                    "@type": clss.ACTIVITY.prefix_name,
                    # context
                    props.DATETIME.name: metadata_activity[props.DATETIME.name],
                    props.CREATOR.name: {"@id": metadata_activity[props.CREATOR.name]},
                    # Job
                    props.FUNCTION.name: {
                        "@id": "QUERY",
                        "@type": clss.FUNCTION.prefix_name,
                        "description": query_sql.__doc__,
                    },
                    props.IDENTIFIER.name: job_id,
                    props.PARAMETER.name: [
                        {
                            "@id": activity_id + "/input/uri",
                            "@type": clss.INPUT.prefix_name,
                            props.NAME_TITLE.name: "uri",
                            props.PARAMETER_VALUE.name: "sqlite:///:memory:",
                        },
                        {
                            "@id": activity_id + "/input/query",
                            "@type": clss.INPUT.prefix_name,
                            props.NAME_TITLE.name: "query",
                            props.PARAMETER_VALUE.name: "select 1 as a",
                        },
                        {
                            "@id": activity_id + "/input/options",
                            "@type": clss.INPUT.prefix_name,
                            props.NAME_TITLE.name: "options",
                            props.PARAMETER_VALUE.name: None,
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
            input_converters=dict.fromkeys(inputs, pickle.loads),
            output_converters=dict.fromkeys(outputs, pickle.dumps),
        )

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

        fid_convert = "urn:function:convert1"
        fid_bytes2json = "bytes2json"

        def generate1() -> bytes:
            return data1

        @AnnotatedFunction.wrap(function_id=fid_convert)
        def convert(data: list) -> list:
            return [x + 1 for x in data]

        loads = AnnotatedFunction(json_loadb, function_id=fid_bytes2json)

        # "output": None -> already bytes
        task_generate = storage.task(
            generate1,
            {"output": None},
            metadata_generator=lambda _: {
                f"{props.FILE.name}.mediatype": "application/json"
            },
            skip_finished=True,
        )
        task_convert = storage.task(
            convert,
            {"output": json_dumpb},
            {"data": loads},
            skip_finished=True,
        )

        key1 = f"generated_{task_generate.get_task_uuid()}.json"
        task_generate(output=key1)
        task_generate(key1)  # does nothing, because already created

        # dynamically create id for next step (use same arguments as in actuall)
        key2 = f"converted_{task_convert.get_task_uuid(data=key1)}.json"
        task_convert(output=key2, data=key1)

        # check metadata
        self.assertEqual(
            get_item_or_first(
                storage.metadata(key2).get(
                    f"{props.GENERATED_BY.name}.{props.FUNCTION.name}.@id"
                )
            ),
            fid_convert,
        )

        self.assertEqual(
            get_item_or_first(
                storage.metadata(key1).get(f"{props.FILE.name}.mediatype")
            ),
            "application/json",
        )
