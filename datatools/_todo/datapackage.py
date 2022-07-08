import logging
import os

from datapackage import Package
from datapackage.exceptions import ValidationError

from .bytes import hash_sha256_file_path, hash_sha256_obj
from .datetime import get_timestamp_utc
from .info import get_user_host
from .json import json_dump, json_load


def pkg_load(file_path):
    descriptor = json_load(file_path)
    base_path = os.path.dirname(file_path)
    pkg = Package(descriptor, base_path=base_path)
    pkg = validate_pkg(pkg)
    return pkg


def validate_pkg(pkg):
    try:
        pkg.valid
    except ValidationError as err:
        logging.error(err.errors)
        raise
    return pkg


def get_pkg_json_file(path):
    if not path.endswith(".json"):
        path += "/datapackage.json"
    return path


def pkg_dump(pkg, file_path):
    # NOTE: because we want to modify
    # the resource descriptors, we have to
    # re-build the package
    if isinstance(pkg, dict):
        base_path = os.path.dirname(file_path)
        pkg = Package(pkg, base_path=base_path)

    descriptor = pkg.descriptor
    base_path = pkg.base_path
    descriptor["datetime_utc"] = get_timestamp_utc()
    descriptor["creator"] = get_user_host()

    for resource_descriptor in descriptor["resources"]:
        if "data" in resource_descriptor:
            sha256 = hash_sha256_obj(resource_descriptor["data"])
        else:
            resource_file_path = base_path + "/" + resource_descriptor["path"]
            sha256 = hash_sha256_file_path(resource_file_path)
        resource_descriptor["sha256"] = sha256

    try:
        pkg = Package(descriptor, strict=True, base_path=base_path)
    except ValidationError as err:
        logging.error(err.errors)
        raise
    json_dump(descriptor, file_path)
