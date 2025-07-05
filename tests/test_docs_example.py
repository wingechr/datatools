# coding: utf-8

import importlib.util
import sys
import unittest


def import_module_from_path(name, filepath):
    spec = importlib.util.spec_from_file_location(name, filepath)
    if spec is None:
        raise ImportError(f"Cannot load module '{name}' from '{filepath}'")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class TestDocs(unittest.TestCase):
    def test_docs_example(self):
        import_module_from_path("example", "docs/example.py")
