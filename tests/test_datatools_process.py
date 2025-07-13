import unittest
from typing import Any

from datatools.process import Function


class TestDatatoolsProcess(unittest.TestCase):

    def test_datatools_proceess_basics(self):

        output = {}

        def get_store_output(key: Any) -> Any:
            def store_output(value: Any, _metadata: Any) -> None:
                output[key] = value

            return store_output

        def get_get_int_input(value: Any) -> Any:
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
