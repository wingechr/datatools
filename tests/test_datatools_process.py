# coding: utf-8

import unittest
from tempfile import TemporaryDirectory
from typing import Callable

from datatools import Converter, Function, Storage


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

        res_inp = storage.ressource("input.json")
        res_outp = storage.ressource("output.json")

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

    def test_datatools_proceess_handler(self):
        url = "http://example.com"

        handler = Converter.convert_to(url, Callable)
        path = "http/example.com/index.html"
        storage = Storage(self.tempdir.name)
        resource = storage.ressource(path)

        function = Function(function=handler)
        process = function.process(url)
        self.assertFalse(resource.exist())
        process(resource)
        self.assertTrue(resource.exist())

        print(resource.metadata.get("$"))
