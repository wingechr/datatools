import logging  # noqa
from datetime import date, datetime, time, timezone

import pytz
import tzlocal

FMT_DATETIME_TZ = "%Y-%m-%dT%H:%M:%S%z"
FMT_DATETIME = "%Y-%m-%dT%H:%M:%S"
FMT_DATE = "%Y-%m-%d"
FMT_TIME = "%H:%M:%S"


def get_timezone_utc():
    return pytz.utc


def _get_timezone_local():
    """e.g.  DstTzInfo 'Europe/Berlin' LMT+0:53:00 STD"""
    return tzlocal.get_localzone()


def get_current_timezone_offset() -> timezone:
    """e.g.  DstTzInfo 'Europe/Berlin' LMT+0:53:00 STD"""
    tz_local = _get_timezone_local()
    now = datetime.now()
    return timezone(tz_local.utcoffset(now))


def to_timezone(dt, tz):
    if dt.tzinfo:  # convert
        return dt.astimezone(tz)
    else:
        return dt.replace(tzinfo=tz)


def now():
    return to_timezone(datetime.now(), get_current_timezone_offset())


def utcnow():
    return to_timezone(datetime.utcnow(), get_timezone_utc())


def fmt_date(dt):
    return dt.strftime(FMT_DATE)


def fmt_time(dt):
    return dt.strftime(FMT_TIME)


def fmt_datetime(dt):
    return dt.strftime(FMT_DATETIME)


def fmt_datetime_tz(dt):
    """the regular strftime does not add a colon in the offset!"""
    result = dt.strftime(FMT_DATETIME_TZ)
    result = result[:-2] + ":" + result[-2:]
    return result


def serialize(x):
    if isinstance(x, datetime):
        if x.tzinfo:
            return fmt_datetime_tz(x)
        else:
            return fmt_datetime(x)
    elif isinstance(x, date):
        return fmt_date(x)
    elif isinstance(x, time):
        return fmt_time(x)
    else:
        raise NotImplementedError(type(x))
