import datetime
import getpass
import hashlib
import os
import socket
from stat import S_IREAD, S_IRGRP, S_IROTH

import tzlocal

DATETIMETZ_FMT = "%Y-%m-%d %H:%M:%S%z"
DATE_FMT = "%Y-%m-%d"


def make_readonly(filepath):
    os.chmod(filepath, S_IREAD | S_IRGRP | S_IROTH)


def get_hash(filepath, method="sha256"):
    hasher = getattr(hashlib, method)()
    with open(filepath, "rb") as file:
        hasher.update(file.read())
    result = {}
    result[method] = hasher.hexdigest()
    return result


def get_now():
    tz_local = tzlocal.get_localzone()
    now = datetime.datetime.now()
    now_tz = now.replace(tzinfo=tz_local)
    return now_tz


def get_now_str():
    return get_now().strftime(DATETIMETZ_FMT)


def get_today_str():
    return get_now().strftime(DATE_FMT)


def get_user_long():
    def get_user():
        """Return current user name"""
        return getpass.getuser()

    def get_host():
        """Return current domain name"""
        # return socket.gethostname()
        return socket.getfqdn()

    return f"{get_user()}@{get_host()}"
