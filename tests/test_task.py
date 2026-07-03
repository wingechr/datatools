"""TODO"""

from unittest import TestCase

from datatools.process.importer import infer_importer_class
from datatools.process.task import Task


class TestImporter(TestCase):
    """TODO"""

    def test_importer_unkown_uri(self):
        """should fail on unknown URI"""
        self.assertRaises(
            NotImplementedError, infer_importer_class, "xyz://bad/protocol"
        )

    def test_invalid_arguments(self):
        """TODO"""

        def test_fun(a, b=1):
            return a + b

        def dump_null(data, name):
            pass

        def read_1(name):
            return 1

        # works
        Task(
            test_fun,
            output_writers={"output": dump_null},
            input_readers={"b": read_1},
        )
        # should not work (output == input)
        self.assertRaises(
            Exception,
            Task,
            test_fun,
            output_writers={"a": dump_null},
        )
        # should not work (invalid input)
        self.assertRaises(
            Exception,
            Task,
            test_fun,
            output_writers={"output": dump_null},
            input_readers={"X": read_1},
        )
