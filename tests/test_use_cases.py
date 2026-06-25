"""TODO"""

from pathlib import Path
import pickle
from tempfile import TemporaryDirectory
from typing import Any
from unittest import TestCase

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
            # should have meta data from import action
            self.assertEqual(get_item_or_first(storage.metadata(uid)["source"]), uri)

            # import from path
            uri = filepath.as_uri()
            uid = storage.import_from_uri(uri)
            self.assertEqual(get_item_or_first(storage.metadata(uid)["source"]), uri)

            # import from sql
            query = "select 1 as a"
            uri = f"sqlite:///:memory:?q={query}"
            uid = storage.import_from_uri(uri)
            self.assertEqual(get_item_or_first(storage.metadata(uid)["query"]), query)

    def test_use_case_cache(self):
        """TODO"""

        storage = MemoryDataStorage()
        global count_calls
        count_calls = 0

        @storage.cache()
        def fun(x, y):
            global count_calls
            count_calls += 1

            return x + y

        self.assertEqual(fun(1, 2), fun(1, 2))
        self.assertEqual(count_calls, 1)
        self.assertEqual(fun(1, y=2), fun(1, y=2))
        self.assertEqual(count_calls, 2)

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

        def make_decorator(storage):
            def load_input1(path_input1: Path) -> Any:
                bytes_input1 = storage[str(path_input1)]
                param_input1 = pickle.loads(bytes_input1)  # noqa:S301
                return param_input1

            def dump_output(path_output: Path, data: Any) -> None:
                bytes_output = pickle.dumps(data)
                storage[str(path_output)] = bytes_output

            def decorator(function):
                # example build tool job
                def job_create_output(*args, **kwargs) -> None:
                    # FIXME kwargs
                    path_output, path_input1, param_input2 = args

                    # optionally: return if exist
                    # (might conflict with build tools decision)
                    if path_output in storage:
                        return

                    global count_calls
                    count_calls += 1

                    # read input(s)
                    param_input1 = load_input1(path_input1)

                    # transform
                    value_output = function(
                        param_input1=param_input1, param_input2=param_input2
                    )

                    # write output(s)
                    dump_output(path_output, value_output)

                return job_create_output

            return decorator

        # generate input1
        storage["input1.pickle"] = pickle.dumps(3)

        def function(param_input1, param_input2):
            return param_input1 + param_input2

        job_create_output = make_decorator(storage)(function)

        # try to call mutliple times - but only of output does not exist
        for _ in range(2):
            if "output.pickle" not in storage:
                job_create_output("output.pickle", "input1.pickle", 10)

        for _ in range(2):
            job_create_output("output.pickle", "input1.pickle", 10)

        self.assertTrue("output.pickle" in storage)
        self.assertEqual(count_calls, 1)
