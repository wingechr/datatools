import json

import frictionless
import genson
import jsonschema

from . import storage


def infer_schema_from_objects(data: list):
    builder = genson.SchemaBuilder()
    builder.add_schema({"type": "object", "properties": {}})
    for item in data:
        builder.add_object(item)
    item_schema = builder.to_schema()
    schema = {"type": "array", "items": item_schema}
    return schema


def validate(data: object, schema: object) -> None:
    if not schema:
        raise Exception("No schema")

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
