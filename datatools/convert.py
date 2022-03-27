import datetime
from decimal import Decimal

FMT_DATES = ["%Y-%m-%d"]
FMT_TIMES = ["%H:%M:%S"]
FMT_DATETIMES = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d",
]


def get_str2dict(dct):
    def f(x):
        return dct[x]

    return f


def str2int(x):
    return int(x)


def str2float(x):
    return float(x)


def str2Decimal(x):
    return Decimal(x)


def int2bool(x):
    f = get_str2dict(
        {
            1: True,
            0: False,
        }
    )
    return f(x)


def str2bool(x):
    f = get_str2dict(
        {
            "1": True,
            "true": True,
            "True": True,
            "TRUE": True,
            "0": False,
            "false": False,
            "False": False,
            "FALSE": False,
        }
    )
    return f(x)


def bool2str(x):
    if x:
        return "true"
    else:
        return "false"


def float2int(x):
    if x.is_integer():
        return int(x)
    else:
        raise ValueError(x)


def strptime(x, patterns):
    for p in patterns:
        try:
            return datetime.datetime.strptime(x, p)
        except Exception:
            pass
    raise ValueError(x)


def str2date(x):
    return strptime(x, FMT_DATES).date()


def str2time(x):
    return strptime(x, FMT_TIMES).time()


def str2datetime(x):
    return strptime(x, FMT_DATETIMES)


def date2str(x):
    return x.strftime(FMT_DATES[0])


def time2str(x):
    return x.strftime(FMT_TIMES[0])


def datetime2str(x):
    return x.strftime(FMT_DATETIMES[0])


def convert(x, to_type):
    from_type = type(x)
    if to_type == from_type:
        return x
    y = {
        (str, bool): str2bool,
        (str, int): str2int,
        (str, float): str2float,
        (str, Decimal): str2Decimal,
        (float, int): float2int,
        (int, float): float,
        (bool, str): bool2str,
        (int, str): str,
        (int, bool): int2bool,
        (float, str): str,
        (str, datetime.date): str2date,
        (str, datetime.time): str2time,
        (str, datetime.datetime): str2datetime,
        (datetime.date, str): date2str,
        (datetime.time, str): time2str,
        (datetime.datetime, str): datetime2str,
    }[(from_type, to_type)](x)
    if type(y) != to_type:
        raise TypeError("%s != %s" % (type(y), to_type))
    return y
