import datetime
from functools import partial
from unittest import TestCase

from datatools.convert import convert


class TmpConvert(TestCase):
    def test_bool(self):
        conv = partial(convert, to_type=bool)
        for x, y in [
            (True, True),
            ("True", True),
            ("true", True),
            ("1", True),
            (1, True),
            (False, False),
            ("False", False),
            ("false", False),
            ("0", False),
            (0, False),
        ]:
            self.assertEqual(conv(x), y)
        for x in [2, -1, "string", None]:
            self.assertRaises(Exception, lambda: conv(x))

    def test_int(self):
        conv = partial(convert, to_type=int)
        for x, y in [("-1", -1), (99, 99), (10.0, 10), (1e3, 1000)]:
            self.assertEqual(conv(x), y)
        for x in ["", "1.1", 1.1, None]:
            self.assertRaises(Exception, lambda: conv(x))

    def test_float(self):
        conv = partial(convert, to_type=float)
        for x, y in [
            ("-0.5", -0.5),
            (0.5, 0.5),
            (10, 10.0),
        ]:
            self.assertEqual(conv(x), y)
        for x in ["", "1,1", None]:
            self.assertRaises(Exception, lambda: conv(x))

    def test_date(self):
        conv = partial(convert, to_type=datetime.date)
        cases_reversable = [("1900-01-02", datetime.date(1900, 1, 2))]

        for x, y in cases_reversable:
            self.assertEqual(conv(x), y)
        for x in ["1900-02-30", "1900-01-01 10:00:00"]:
            self.assertRaises(Exception, lambda: conv(x))

        # inverse
        conv = partial(convert, to_type=str)
        for y, x in cases_reversable:
            self.assertEqual(conv(x), y)

    def test_time(self):
        conv = partial(convert, to_type=datetime.time)
        cases_reversable = [
            ("10:11:12", datetime.time(10, 11, 12)),
        ]

        for x, y in cases_reversable:
            self.assertEqual(conv(x), y)
        for x in ["1900-02-30", "1900-01-01 10:00:00"]:
            self.assertRaises(Exception, lambda: conv(x))

        # inverse
        conv = partial(convert, to_type=str)
        for y, x in cases_reversable:
            self.assertEqual(conv(x), y)

    def test_datetime(self):
        conv = partial(convert, to_type=datetime.datetime)

        for x, y in [
            ("1900-01-02", datetime.datetime(1900, 1, 2)),
            ("1900-01-02 03:04:05", datetime.datetime(1900, 1, 2, 3, 4, 5)),
            ("1900-01-02 03:04:05.0", datetime.datetime(1900, 1, 2, 3, 4, 5)),
            (
                "1900-01-02T03:04:05.123",
                datetime.datetime(1900, 1, 2, 3, 4, 5, 123000),
            ),  # !!
            (
                "1900-01-02T03:04:05.123456",
                datetime.datetime(1900, 1, 2, 3, 4, 5, 123456),
            ),
        ]:
            self.assertEqual(conv(x), y)

        conv = partial(convert, to_type=str)
        for y, x in [
            ("1900-01-02 00:00:00", datetime.datetime(1900, 1, 2)),
            ("1900-01-02 03:04:05", datetime.datetime(1900, 1, 2, 3, 4, 5)),
        ]:
            self.assertEqual(conv(x), y)
