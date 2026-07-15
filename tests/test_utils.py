"""TODO"""

from datatools.utils import (
    DEFAULT_ENCODING,
    BufferIter,
    is_file_readonly,
    make_file_readonly,
    make_file_writable,
)

from .base import TempdirTestCase


class TestUtils(TempdirTestCase):
    """TODO"""

    def test_file_readonly(self):
        """TODO"""
        f = self.temp_dir / "test.txt"
        f.touch()
        self.assertFalse(is_file_readonly(f))
        with f.open("w", encoding=DEFAULT_ENCODING) as file:
            file.write("test")

        make_file_readonly(f)
        self.assertTrue(is_file_readonly(f))
        self.assertRaises(Exception, f.open, "w")
        with f.open("r", encoding=DEFAULT_ENCODING) as file:
            self.assertEqual(file.read(), "test")

        make_file_writable(f)
        self.assertFalse(is_file_readonly(f))
        with f.open("w", encoding=DEFAULT_ENCODING) as file:
            file.write("test")

    def test_BufferIter(self):
        """TODO

        ensure that error in writing thread
        does not cause the pipeline to get stuck
        """

        def f(_, buf):
            buf.write(b"data1")
            buf.write(b"data2")
            raise ValueError()

        def consume_iterator():
            list(BufferIter(f)(None))

        self.assertRaises(ValueError, consume_iterator)
