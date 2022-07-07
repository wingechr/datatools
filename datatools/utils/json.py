import datetime
import json


def serialize(x):
    if isinstance(x, datetime.datetime):
        return x.strftime("%Y-%m-%dT%H:%M:%S%z")
    elif isinstance(x, datetime.date):
        return x.strftime("%Y-%m-%d")
    elif isinstance(x, datetime.time):
        return x.strftime("%H:%M:%S")
    else:
        raise NotImplementedError(type(x))


def dumps(data, serialize=None):
    return json.dumps(
        data, indent=2, sort_keys=True, ensure_ascii=False, default=serialize
    )


def dump(data, filepath: str, serialize=None):
    data_s = dumps(data, serialize=serialize)
    with open(filepath, "w", encoding="utf-8") as file:
        file.write(data_s)


def loads(data: str):
    return json.loads(data)


def load(filepath: str):
    with open(filepath, "r", encoding="utf-8") as file:
        data_s = file.read()
    return loads(data_s)
