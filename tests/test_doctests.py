"""Detect and run all doctests."""

import doctest
import glob
import importlib.util
import os
import unittest


class TestDocstests(unittest.TestCase):
    """Detect and run all doctests."""

    def test_docstests(self):
        """Detect and run all doctests."""

        for file in glob.glob("datatools/*.py"):
            module_name = os.path.splitext(file)[0]
            spec = importlib.util.spec_from_file_location(module_name, file)
            if not spec or not spec.loader:
                raise ImportError()
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            doctest.testmod(module, verbose=True, raise_on_error=True)
