"""TODO"""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from datatools.storage.classes import MemoryDataStorage
from tests import start_http_server


def get_item_or_first(x):
    """TODO"""
    if isinstance(x, list):
        return x[0]
    return x


class TestUseCases(TestCase):
    """TODO"""

    def test_use_case_import_data(self):
        """TODO"""

        test_data = "äöü".encode("iso-8859-1")
        filename = "data.txt"
        storage = MemoryDataStorage()

        with TemporaryDirectory() as tmpdir:
            # create test file
            filepath = Path(tmpdir) / filename
            filepath.write_bytes(test_data)
            base_url = start_http_server(tmpdir)

            # import from http source
            uri = base_url + "/" + filename
            uid = storage.import_from_uri(uri)
            # should have meta data from import action
            self.assertEqual(get_item_or_first(storage.metadata(uid)["source"]), uri)

            # import from path
            uri = filepath.as_uri()
            uid = storage.import_from_uri(uri)
            self.assertEqual(get_item_or_first(storage.metadata(uid)["source"]), uri)

            # import from sql
            query = "select 1 as a"
            uri = f"sqlite:///:memory:?q={query}"
            uid = storage.import_from_uri(uri)
            self.assertEqual(get_item_or_first(storage.metadata(uid)["query"]), query)
