"""Detect and run all doctests."""

import doctest
import glob
import importlib
import os
import unittest


class TestDocstests(unittest.TestCase):
    """Detect and run all doctests."""

    def test_docstests(self):
        """Detect and run all doctests."""

        for file in glob.glob("datatools/*.py"):
            module_name = os.path.splitext(file)[0].replace(os.sep, ".")
            module = importlib.import_module(module_name)
            doctest.testmod(module, verbose=True, raise_on_error=True)
