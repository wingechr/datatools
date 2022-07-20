import json
import logging  # noqa

import frictionless
import jsonschema

import datatools

from .byte import hash as byte_hash

SCHEMA_SUFFIX = ".schema.json"
RESOURCE_TMP_NAME = "_name"


def load_schema(uri):
    return datatools.location.location(uri).read(as_json=True)


def validate_json_schema(data, json_schema: str | dict | bool = True) -> object:
    if json_schema is True:
        json_schema = data["$schema"]
    if isinstance(json_schema, str):
        json_schema = load_schema(json_schema)

    jsonschema.validate(data, json_schema)
    logging.debug("Validation ok")

    return data


def validate_table_schema(data, table_schema):
    if isinstance(table_schema, str):
        table_schema = load_schema(table_schema)
    validate_resource({"data": data, "schema": table_schema, "name": RESOURCE_TMP_NAME})

    return data


def infer_table_schema(data):
    res = validate_resource({"data": data, "name": RESOURCE_TMP_NAME})
    tasks = res["tasks"]
    assert len(tasks) == 1
    schema = tasks[0]["resource"]["schema"]
    return schema


def validate_resource(resource_descriptor):
    frictionless.Resource(descriptor=resource_descriptor, onerror="raise")
    rep = frictionless.validate_resource(descriptor=resource_descriptor)
    if not rep["valid"]:
        raise Exception(rep)
    return rep


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
