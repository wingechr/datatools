import datetime
import json


def json_serialize(x):
    if isinstance(x, datetime.datetime):
        return x.strftime("%Y-%m-%dT%H:%M:%S%z")
    elif isinstance(x, datetime.date):
        return x.strftime("%Y-%m-%d")
    elif isinstance(x, datetime.time):
        return x.strftime("%H:%M:%S")
    else:
        raise NotImplementedError(type(x))


def json_dump(data, filepath: str):
    data_s = json.dumps(
        data, indent=2, sort_keys=True, ensure_ascii=False, default=json_serialize
    )
    with open(filepath, "w", encoding="utf-8") as file:
        file.write(data_s)


def json_load(filepath: str):
    with open(filepath, "r", encoding="utf-8") as file:
        return json.load(file)
