import datetime


def get_timestamp_utc():
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+00:00")
