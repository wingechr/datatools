from datatools.utils import HashedByteIterator

from . import TestCase


class TestHashedByteIterator(TestCase):
    def test_filepath(self):
        filepath = self.get_data_filepath("data.txt")

        chunks1 = []
        with open(filepath, "rb") as file:
            for chunk in HashedByteIterator(file, chunk_size=2, max_bytes=5):
                chunks1.append(chunk)

        chunks2 = []
        for chunk in HashedByteIterator(filepath, chunk_size=2, max_bytes=5):
            chunks2.append(chunk)

        chunks3 = []
        with open(filepath, "rb") as file:
            data = file.read()
        for chunk in HashedByteIterator(data, chunk_size=2, max_bytes=5):
            chunks3.append(chunk)

        self.assertEqual(tuple(chunks1), tuple(chunks2))
        self.assertEqual(tuple(chunks1), tuple(chunks3))
