import json


def dumps(data, serialize=None):
    return json.dumps(
        data, indent=2, sort_keys=True, ensure_ascii=False, default=serialize
    )


def dump(data, file_path: str, serialize=None):
    data_s = dumps(data, serialize=serialize)
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(data_s)


def loads(data: str):
    return json.loads(data)


def load(file_path: str):
    with open(file_path, "r", encoding="utf-8") as file:
        data_s = file.read()
    return loads(data_s)
