import datetime

from datatools import utils

from . import TestCase


class TestSchema(TestCase):
    def test_hash(self):
        self.assertEqual(
            utils.byte.hash(b"", "sha256"),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        self.assertEqual(
            utils.byte.hash(b"test", "sha256"),
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
        )
        # b'"test"'
        self.assertEqual(
            utils.json.hash("test", "sha256"),
            "4d967a30111bf29f0eba01c448b375c1629b2fed01cdfcc3aed91f1b57d5dd5e",
        )
        # b'null'
        self.assertEqual(
            utils.json.hash(None, "sha256"),
            "74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b",
        )


class TestScons(TestCase):
    def test_datetime(self):
        dt_unaware = datetime.datetime.now()
        tz_offset = utils.datetime.get_current_timezone_offset()
        tc_utc = utils.datetime.get_timezone_utc()
        dt_aware_local = utils.datetime.to_timezone(dt_unaware, tz_offset)
        dt_aware_utc = utils.datetime.to_timezone(dt_aware_local, tc_utc)
        self.assertTrue(dt_aware_local.tzinfo)
        self.assertEqual(dt_aware_local, dt_aware_utc)
        now = utils.datetime.now()
        utcnow = utils.datetime.utcnow()
        self.assertTrue((utcnow - now).seconds < 10)
