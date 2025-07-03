# coding: utf-8
import logging
import unittest

from datatools import Process

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.INFO
)


class TestDatatoolsProcess(unittest.TestCase):
    def test_datatools_proceess_basics(self):

        output = {}

        def get_store_output(key):
            def store_output(value):
                output[key] = value

            return store_output

        def get_get_input(value):
            def get_input():
                return value

            return get_input

        def function(*args, **kwargs):
            return (sum(args), sum(kwargs.values()))

        # input output: single
        proc = Process(
            function=function,
            inputs=get_get_input(1),
            outputs=get_store_output("test1"),
        )
        proc()
        self.assertEqual(output["test1"], (1, 0))

        # input output: single
        proc = Process(
            function=function,
            inputs={0: get_get_input(2), 1: get_get_input(3), "x": get_get_input(4)},
            outputs=[get_store_output("test2.1"), get_store_output("test2.2")],
        )
        proc()
        self.assertEqual(output["test2.1"], 2 + 3)
        self.assertEqual(output["test2.2"], 4)
