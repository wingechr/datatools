import json
import logging
from os import makedirs
from os.path import dirname, isfile
from typing import Union

import frictionless
import jsonschema
import requests


def validate(data: object, schema: object) -> None:
    pass


def load_schema():
    raise NotImplementedError


def get_local_path():
    raise NotImplementedError


def validate_resource(resource_descriptor):
    res = frictionless.Resource(resource_descriptor)
    rep = res.validate()

    if rep.stats["errors"]:
        errors = []
        for task in rep.tasks:
            for err in task["errors"]:
                errors.append(err["message"])

        err_str = "\n".join(errors)
        # logging.error(err_str)
        raise ValueError(err_str)


def get_jsonschema(schema_url, cache_dir=None, encoding="utf-8"):
    local_path = get_local_path(schema_url, cache_dir=cache_dir)
    if not isfile(local_path):
        makedirs(dirname(local_path), exist_ok=True)
        res = requests.get(schema_url)
        res.raise_for_status()
        res = json.dumps(res.json(), indent=4, ensure_ascii=False)
        with open(local_path, "w", encoding=encoding) as file:
            file.write(res)
    with open(local_path, "r", encoding=encoding) as file:
        return json.load(file)


def get_jsonschema_validator(schema):
    """Return validator instance for schema.

    Example:

    >>> schema = {"type": "object", "properties": {"id": {"type": "integer"}}, "required": [ "id" ]}  # noqa
    >>> validator = get_jsonschema_validator(schema)
    >>> validator({})
    Traceback (most recent call last):
        ...
    ValueError: 'id' is a required property ...

    >>> validator({"id": "a"})
    Traceback (most recent call last):
        ...
    ValueError: 'a' is not of type 'integer' ...

    >>> validator({"id": 1})

    """

    if isinstance(schema, str):
        schema = get_jsonschema(schema)

    validator_cls = jsonschema.validators.validator_for(schema)
    # check if schema is valid
    validator_cls.check_schema(schema)
    validator = validator_cls(schema)

    def validator_function(instance):
        errors = []
        for err in validator.iter_errors(instance):
            # path in data structure where error occurs
            path = "$" + "/".join(str(x) for x in err.absolute_path)
            errors.append("%s in %s" % (err.message, path))
        if errors:
            err_str = "\n".join(errors)
            # logging.error(err_str)
            raise ValueError(err_str)

    return validator_function


def validate_json_schema(data, schema: Union[str, dict, bool] = True) -> object:
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
