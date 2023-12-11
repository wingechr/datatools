import json
from typing import Union

import frictionless
import genson
import jsonschema
import pandas as pd

from . import storage
from .exceptions import SchemaError, ValidationError
from .utils import get_err_message


def infer_schema_from_objects(data: list):
    builder = genson.SchemaBuilder()
    builder.add_schema({"type": "object", "properties": {}})
    for item in data:
        builder.add_object(item)
    item_schema = builder.to_schema()
    schema = {"type": "array", "items": item_schema}
    return schema


def validate(data: Union[list, pd.DataFrame], schema: dict) -> None:
    """validate date against schema

    Parameters
    ----------
    data : Union[list,pd.DataFrame]
        data object: either a list of dicts or a DataFrame
    schema : dict
        schema object, either a json schema or a frictionless table schema

    Raises
    ------
    Exception
        _description_
    NotImplementedError
        _description_
    """
    if not schema:
        raise Exception("No schema")

    if isinstance(data, pd.DataFrame):
        pass

    if is_jsonschema(schema):
        validator = get_jsonschema_validator(schema)
        validator(data)
    elif is_frictionlessschema(schema):
        resource = {
            "name": "todo",
            "schema": schema,
            "profile": "tabular-data-resource",
            "data": data,
        }
        validate_resource(resource)
    else:
        raise NotImplementedError()


def is_jsonschema(schema: object):
    return "type" in schema


def is_frictionlessschema(schema: object):
    return "fields" in schema


def validate_resource(resource_descriptor):
    try:
        res = frictionless.Resource(resource_descriptor)
    except frictionless.exception.FrictionlessException as exc:
        raise SchemaError(exc)

    rep = res.validate()

    if rep.stats["errors"]:
        errors = []
        for report_task in rep.tasks:
            for err in report_task.errors:
                err_msg = get_err_message(err)
                errors.append(err_msg)
        err_str = "\n".join(errors)
        raise ValidationError(err_str)


def get_jsonschema_storage():
    return storage.StorageGlobal()


def get_jsonschema(schema_url):
    with get_jsonschema_storage().resource(source_uri=schema_url).open() as file:
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
    try:
        validator_cls.check_schema(schema)
    except Exception as exc:
        raise SchemaError(exc)
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
            raise ValidationError(err_str)

    return validator_function
