import json
import logging  # noqa

import jsonschema
import requests
import requests_cache
from frictionless.validate import validate_resource

from ..utils.byte import hash as byte_hash

SCHEMA_SUFFIX = ".schema.json"

requests_cache.install_cache("datatools_schema_cache", backend="sqlite", use_temp=True)


def load_schema(uri: str) -> object:
    if uri.startswith("http://") or uri.startswith("https://"):
        return requests.get(uri).json()
    # local file
    return load(uri)


def validate_jsonschema(data, schema: str | dict | bool = True) -> object:
    if isinstance(schema, str):
        schema = load_schema(schema)
    elif isinstance(schema, dict):
        pass
    elif schema is True:
        uri = data["$schema"]
        schema = load_schema(uri)
    else:
        raise NotImplementedError(schema)

    jsonschema.validate(data, schema)

    return data


def validate_dataschema(data, schema):
    res = validate_resource({"data": data, "schema": schema})
    print(res)
    return data


class SchemaValidator:
    __slots__ = []

    def __init__(self, schema):
        if isinstance(schema, str):
            schema = load_schema(schema)
        self.schema = schema

    def validate(self, json):
        return validate_jsonschema(json, self.schema)


def dumps(data: object, serialize=None) -> str:
    return json.dumps(
        data, indent=2, sort_keys=True, ensure_ascii=False, default=serialize
    )


def dump(data: object, file_path: str, serialize=None) -> None:
    str_data = dumps(data, serialize=serialize)
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(str_data)


def loads(str_data: str) -> object:
    return json.loads(str_data)


def loadb(bytes_data: bytes, encoding: str = "utf-8") -> object:
    str_data = bytes_data.decode(encoding=encoding)
    return loads(str_data)


def dumpb(data: object) -> bytes:
    return dumps(data).encode()


def load(file_path: str) -> object:
    with open(file_path, "r", encoding="utf-8") as file:
        data_s = file.read()
    return loads(data_s)


def hash(data, method="sha256") -> str:
    bytes_data = dumpb(data)
    return byte_hash(bytes_data, method=method)


def validate_data(data, schema):
    pass
