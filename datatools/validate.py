import jsonschema
import requests
import requests_cache

requests_cache.install_cache("datatools_schema_cache", backend="sqlite", use_temp=True)


def load_schema(uri: str) -> object:
    return requests.get(uri).json()


def validate_json(json, schema=None) -> object:
    if not schema:
        uri = json["$schema"]
        schema = load_schema(uri)

    jsonschema.validate(json, schema)
    return json
