import json
import logging  # noqa

import jsonschema
import requests
import requests_cache

from .. import utils  # noqa

SCHEMA_SUFFIX = ".schema.json"

requests_cache.install_cache("datatools_schema_cache", backend="sqlite", use_temp=True)


def load_schema(uri: str) -> object:
    if uri.startswith("http://") or uri.startswith("https://"):
        return requests.get(uri).json()
    # local file
    return utils.json.load(uri)


def validate(json, schema=None) -> object:
    if not schema:
        uri = json["$schema"]
        schema = load_schema(uri)
    elif isinstance(schema, str):
        schema = load_schema(schema)

    jsonschema.validate(json, schema)
    return json


class Validator:
    def __init__(self, schema):
        if isinstance(schema, str):
            schema = load_schema(schema)
        self.schema = schema

    def validate(self, json):
        return validate(json, self.schema)


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


def hash(json_data, method="sha256") -> str:
    bytes_data = dumps(json_data).encode()
    return utils.byte.hash(bytes_data, method=method)
