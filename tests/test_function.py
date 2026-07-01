"""TODO"""

from unittest import TestCase

from datatools.job.importer import infer_importer_class


class TestImporter(TestCase):
    """TODO"""

    def test_importer_unkown_uri(self):
        """should fail on unknown URI"""
        self.assertRaises(
            NotImplementedError, infer_importer_class, "xyz://bad/protocol"
        )
