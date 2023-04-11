"""
We want to
    * cache json schemas locally (because speed and behind proxy)
    * deref them into single files

Example:
    assert_url_local("http://swagger.io/v2/schema.json")

"""

import json
from os import makedirs
from os.path import dirname, isfile
from urllib.parse import urljoin

from dataschema import get_jsonschema

DATA_DIR = dirname(__file__) + "/source"
ENCODING = "utf-8"


def url2json(url):
    return get_jsonschema(url, cache_dir=DATA_DIR, encoding=ENCODING)


def _load_deref(o, url, base_url=None):
    def x(o: dict):
        res = dict((k, _load_deref(v, url, base_url)) for k, v in o.items())
        if "$ref" in res and isinstance(res["$ref"], str):
            if not res["$ref"].startswith("#"):
                res_orig = res
                ref = res_orig.pop("$ref")
                ref_url = urljoin(base_url or url, ref)
                res = url2json(ref_url)
                res = _load_deref(res, ref_url, base_url)
                # merge other values(except $ref)
                res.update(res_orig)
            else:
                # local ref
                if not res["$ref"].startswith("#/"):
                    raise NotImplementedError(
                        "relative nested refs not yet implemented"
                    )
        return res

    if isinstance(o, dict):
        return x(o)
    elif isinstance(o, list):
        return [_load_deref(v, url, base_url) for v in o]
    else:
        return o


def load_deref(local_path, url, base_url=None):
    if not isfile(local_path):
        makedirs(dirname(local_path), exist_ok=True)
        schema = _load_deref({"$ref": url}, url, base_url)
        if "$id" not in schema:
            schema["$id"] = url
        assert schema["$id"] == url
        schema_str = json.dumps(schema, indent=2, ensure_ascii=False, sort_keys=True)
        with open(local_path, "w", encoding=ENCODING) as file:
            file.write(schema_str)
    with open(local_path, "r", encoding=ENCODING) as file:
        return json.load(file)
