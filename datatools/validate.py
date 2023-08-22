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


def validate_resource(resource):
    frictionless.Resource(descriptor=resource)
    res = frictionless.validate_resource(resource)
    if not res["valid"]:
        raise Exception(res)
    return res
