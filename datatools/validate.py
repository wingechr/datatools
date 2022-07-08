import jsonschema
import requests
import requests_cache

from datatools import utils

SCHEMA_SUFFIX = ".schema.json"

requests_cache.install_cache("datatools_schema_cache", backend="sqlite", use_temp=True)


def load_schema(uri: str) -> object:
    if uri.startswith("http://") or uri.startswith("https://"):
        return requests.get(uri).json()
    # local file
    return utils.json.load(uri)


def validate_json(json, schema=None) -> object:
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

    def validate_json(self, json):
        return validate_json(json, self.schema)
