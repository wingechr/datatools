# coding: utf-8

import unittest

from datatools.utils import import_module_from_path


class TestDocs(unittest.TestCase):
    def test_docs_example(self):
        import_module_from_path("example", "docs/example.py")
