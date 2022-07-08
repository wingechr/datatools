import datetime
import json
import logging

ENCODING = "utf-8"


def json_serialize(x):
    if isinstance(x, datetime.datetime):
        return x.strftime("%Y-%m-%dT%H:%M:%S%z")
    elif isinstance(x, datetime.date):
        return x.strftime("%Y-%m-%d")
    elif isinstance(x, datetime.time):
        return x.strftime("%H:%M:%S")
    else:
        raise NotImplementedError(type(x))


def json_dump(data, file_path):
    data_s = json_dumps(data)
    logging.debug("WRITE %s", file_path)
    with open(file_path, "w", encoding=ENCODING) as file:
        file.write(data_s)


def json_dumps(data):
    return json.dumps(data, indent=2, sort_keys=True, ensure_ascii=False, default=None)


def json_load(file_path):
    logging.debug("READ %s", file_path)
    with open(file_path, "r", encoding=ENCODING) as file:
        return json.load(file)
