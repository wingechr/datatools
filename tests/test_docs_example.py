"""Run example script from documentation."""

import importlib
import importlib.util
import os
import unittest

import datatools


class TestDocsExample(unittest.TestCase):
    """Run example script from documentation."""

    def test_docs_example_py(self):
        """Run example script from docs/example.py"""

        file = "docs/example.py"

        module_name = os.path.splitext(file)[0]
        spec = importlib.util.spec_from_file_location(module_name, file)
        if not spec or not spec.loader:
            raise ImportError()
        module = importlib.util.module_from_spec(spec)
        # should not raise any error
        spec.loader.exec_module(module)

    def test_main_test(self):
        """Run tets() function from main"""
        self.assertTrue(datatools.self_check())
