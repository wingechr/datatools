"""Run example script from documentation."""

import subprocess
import unittest


class TestDocsExample(unittest.TestCase):
    """Run example script from documentation."""

    def test_docs_example_py(self):
        """Run example script from docs/example.py"""
        subprocess.check_call(["python", "docs/example.py"])  # noqa: S607
