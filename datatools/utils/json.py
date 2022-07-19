import json
import logging  # noqa

import frictionless
import jsonschema

import datatools

from ..utils.byte import hash as byte_hash

SCHEMA_SUFFIX = ".schema.json"


def load_schema(uri):
    return datatools.location.location(uri).read(as_json=True)


def validate_json_schema(data, schema: str | dict | bool = True) -> object:
    if schema is True:
        schema = data["$schema"]
    if isinstance(schema, str):
        schema = load_schema(schema)

    jsonschema.validate(data, schema)
    logging.debug("Validation ok")

    return data


def validate_resource_schema(data, schema):
    if not schema:
        raise Exception("no schema")
    validate_resource({"data": data, "schema": schema})
    return data


def guess_data_schema(data):
    res = validate_resource({"data": data})
    tasks = res["tasks"]
    assert len(tasks) == 1
    schema = tasks[0]["resource"]["schema"]
    return schema


def validate_resource(resource):
    frictionless.Resource(descriptor=resource)
    res = frictionless.validate_resource(resource)
    if not res["valid"]:
        raise Exception(res)
    return res


class SchemaValidator:
    __slots__ = ["schema", "validator"]

    def __init__(self, schema):
        if isinstance(schema, str):
            schema = load_schema(schema)

        validator_cls = jsonschema.validators.validator_for(schema)
        # check if schema is valid
        validator_cls.check_schema(schema)
        self.validator = validator_cls(schema)

        self.schema = schema

    def validate(self, json):
        return self.validator.validate(json)


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
