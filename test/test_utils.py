from datatools.utils.bytes import hash_bytes, hash_json

from . import TestCase


class TestSchema(TestCase):
    def test_hash(self):
        self.assertEqual(
            hash_bytes(b"", "sha256"),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        self.assertEqual(
            hash_bytes(b"test", "sha256"),
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
        )
        # b'"test"'
        self.assertEqual(
            hash_json("test", "sha256"),
            "4d967a30111bf29f0eba01c448b375c1629b2fed01cdfcc3aed91f1b57d5dd5e",
        )
        # b'null'
        self.assertEqual(
            hash_json(None, "sha256"),
            "74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b",
        )
