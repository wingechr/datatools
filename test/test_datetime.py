import datetime

from datatools import utils

from . import TestCase


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
