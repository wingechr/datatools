# coding: utf-8

import unittest
from tempfile import TemporaryDirectory
from typing import cast

from datatools import Function, Storage
from datatools.process import Process, Resource


class TestDatatoolsProcess(unittest.TestCase):
    def setUp(self):
        self.tempdir = TemporaryDirectory()

    def tearDown(self):
        self.tempdir.cleanup()

    def test_datatools_proceess_basics(self):

        output = {}

        def get_store_output(key):
            def store_output(value, _metadata):
                output[key] = value

            return store_output

        def get_get_int_input(value):
            def get_input():
                return value

            return get_input

        # create a function object
        @Function.wrap()
        def function(*args: int, **kwargs: float) -> tuple[int, float]:
            return (sum(args), sum(kwargs.values()))

        # wrapped function still should work normally
        self.assertEqual(function(1, 2, a=3, b=4), (3, 7))

        # bind inputs to create process
        process = function.process(get_get_int_input(1))
        # run process
        process(get_store_output("test1"))
        self.assertEqual(output["test1"], (1, 0))

    def test_datatools_proceess_resource(self):

        storage = Storage(self.tempdir.name)

        res_inp = storage.resource("input.json")
        res_outp = storage.resource("output.json")

        res_inp.dump([1, 2, 3])

        def function(data: list, factor: int) -> list:
            return data * factor

        func = Function(function=function)
        proc = func.process(res_inp, 10)

        self.assertFalse(res_outp.exist())
        proc(res_outp)
        self.assertTrue(res_outp.exist())
        # cannot run process again, because resource already exists
        self.assertRaises(Exception, proc, res_outp)

    def __test_datatools_proceess_storage(self):

        storage = Storage(self.tempdir.name)

        res_inp = storage.resource("input.json")
        res_inp.dump([1, 2, 3])

        def function(data: list, factor: int) -> list:
            return data * factor

        func = Function(function=function)
        proc = func.process(res_inp, 10)

        # use Storage as output: auto generate resource name from output uri
        # TODO: does not work yet because converter detection requires
        # knowledge of filetype
        res_outp = cast(Resource, proc(storage))
        self.assertTrue(isinstance(res_outp, Storage))
        self.assertTrue(res_outp.exist())

    def __test_datatools_proceess_uri(self):
        uri = "http://example.com"
        process = Process.from_uri(uri)

        storage = Storage(self.tempdir.name)
        resource = storage.resource(name=uri)

        self.assertFalse(resource.exist())
        process(resource)
        self.assertTrue(resource.exist())

        print(resource.metadata.get("$"))
