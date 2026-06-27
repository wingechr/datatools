"""TODO"""

import datetime
import json
from pathlib import Path
import pickle
from tempfile import TemporaryDirectory
from unittest import TestCase

from datatools.job.classes import FunctionWrapper
from datatools.storage.classes import MemoryDataStorage
from tests import start_http_server


def get_item_or_first(x):
    """TODO"""
    if isinstance(x, list):
        return x[0]
    return x


class TestUseCases(TestCase):
    """TODO"""

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
            uri = f"sqlite:///:memory:?q={query}"
            uid = storage.import_from_uri(uri)
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
        """TODO"""
        storage = MemoryDataStorage()

        data1 = b"[1, 2]"
        key1 = "result1"
        key2 = "result2"
        fid_convert = "function://convert1"
        fid_bytes2json = "bytes2json"

        def generate1() -> bytes:
            return data1

        @FunctionWrapper.wrap(function_id=fid_convert)
        def convert(data: list) -> list:
            return [x + 1 for x in data]

        loads = FunctionWrapper(json.loads, function_id=fid_bytes2json)

        job_generate = storage.job(generate1, {"output": None}, check_done=True)
        job_convert = storage.job(
            convert, {"output": json.dumps}, {"data": loads}, check_done=True
        )

        job_generate(output=key1)
        job_generate(key1)  # does nothing
        job_convert(output=key2, data=key1)

        # check metadata
        self.assertEqual(
            get_item_or_first(storage.metadata(key1)["origin.converter"]),
            "identity",  # nothings
        )
        self.assertEqual(
            get_item_or_first(storage.metadata(key2)["origin.function.@id"]),
            fid_convert,
        )
        self.assertEqual(
            get_item_or_first(
                storage.metadata(key2)["origin.parameter.data.converter"]
            ),
            fid_bytes2json,
        )
