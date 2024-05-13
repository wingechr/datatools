"""Test example usage script.
(used in doc and README)
"""

import unittest


class TestExampleUsage(unittest.TestCase):
    def test_example_usage(self):
        # just import the script, should run without error
        from docs import example_usage  # noqa
