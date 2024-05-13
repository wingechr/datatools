"""Test example usage script.
(used in doc and README)
"""

import doctest
import unittest

import datatools.generators
import datatools.loaders
import datatools.storage
import datatools.utils

# specify modules to test
modules = [datatools.utils, datatools.storage, datatools.generators, datatools.loaders]


class Test(unittest.TestCase):
    def test_doctest(self):
        """Run doctests."""
        for mod in modules:
            self.assertFalse(doctest.testmod(mod).failed, mod)
