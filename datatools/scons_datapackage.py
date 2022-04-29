import logging
import os
import re
from urllib.parse import urlparse

from datapackage import Package
from datapackage.exceptions import ValidationError

from .bytes import hash_sha256_filepath, hash_sha256_obj
from .datetime import get_timestamp_utc
from .info import get_user_host
from .json import json_dump, json_load
from .logging import show_trace
from .path import copy_uri, get_filepath_uri
from .requests import download_file
from .sql import download_sql
from .zipfile import unzip_file


def get(target_pkg, source_pkg):
    source_base_path = source_pkg.base_path
    for resource in source_pkg.resources:

        resource_filepath = get_resource_filepath(resource.descriptor)
        target_filepath = target_pkg.base_path + "/data/" + resource_filepath
        target_dir = os.path.dirname(target_filepath)
        os.makedirs(target_dir, exist_ok=True)
        source_uri = resource.descriptor["data"]
        source_scheme = urlparse(source_uri).scheme
        if re.match(r"^file$", source_scheme):
            copy_uri(source_uri, target_filepath, source_base_path=source_base_path)
        elif re.match(r"^http[s]$", source_scheme):
            download_file(source_uri, target_filepath)
        elif re.match(r"^.*sql.*$", source_scheme):
            download_sql(source_uri, target_filepath)
        else:
            raise NotImplementedError(source_scheme)
        target_pkg.add_resource(
            {
                "name": resource.name,
                "file": resource_filepath,
                "source": source_uri,
                "path": "data/" + resource_filepath,
            }
        )


def unzip(target_pkg, source_pkg):
    target_data_dir = target_pkg.base_path + "/data"
    for resource in source_pkg.resources:
        source_zipfile = source_pkg.base_path + "/" + resource.descriptor["path"]
        source_sha256 = resource.descriptor["sha256"]
        for name in unzip_file(source_zipfile, target_data_dir):
            target_pkg.add_resource(
                {
                    "name": name,
                    "source": get_filepath_uri(source_zipfile),
                    "source_sha256": source_sha256,
                    "path": "data/" + name,
                }
            )


def get_resource_filepath(resource_descriptor):
    if "path" in resource_descriptor:
        path = resource_descriptor["path"]
    elif "file" in resource_descriptor:
        path = "data/" + resource_descriptor["file"]
    else:
        path = "data/" + resource_descriptor["name"]
    return path


def get_pkg_file(path):
    if not path.endswith(".json"):
        path += "/datapackage.json"
    return path


def pkg_load(filepath):
    descriptor = json_load(filepath)
    base_path = os.path.dirname(filepath)
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


def pkg_dump(pkg, filepath):
    # NOTE: because we want to modify
    # the resource descriptors, we have to
    # re-build the package

    descriptor = pkg.descriptor
    base_path = pkg.base_path
    descriptor["datetime_utc"] = get_timestamp_utc()
    descriptor["creator"] = get_user_host()

    for resource_descriptor in descriptor["resources"]:
        if "data" in resource_descriptor:
            sha256 = hash_sha256_obj(resource_descriptor["data"])
        else:
            resource_filepath = (
                base_path + "/" + get_resource_filepath(resource_descriptor)
            )
            print(
                resource_filepath, base_path, get_resource_filepath(resource_descriptor)
            )
            sha256 = hash_sha256_filepath(resource_filepath)
        resource_descriptor["sha256"] = sha256

    try:
        pkg = Package(descriptor, strict=True, base_path=base_path)
    except ValidationError as err:
        logging.error(err.errors)
        raise
    json_dump(descriptor, filepath)


class DatapackageBuilder:
    def __init__(self, env):
        self._env = env

    def __call__(self, func, target_pkg_path, *positional_sources, **named_sources):
        @show_trace
        def wrapped_func(target, source, env):
            if len(target) != 1:
                raise Exception("Only works with single target")
            target_path = env.GetBuildPath(target[0])
            sources_paths = [env.GetBuildPath(p) for p in source]
            sources_pkgs = [pkg_load(s) for s in sources_paths]
            named_sources_names = tuple(named_sources.keys())
            n_positional = len(sources_pkgs) - len(named_sources_names)
            positional_sources_pkgs = sources_pkgs[:n_positional]
            named_sources_pkgs = dict(
                zip(named_sources_names, sources_pkgs[n_positional:])
            )
            basename = os.path.basename(target_path)
            if basename == "datapackage.json":
                # use folder name
                name = os.path.basename(os.path.dirname(target_path))
            else:
                name = re.sub(r"\.json$", "", basename)
            pkg = Package({"name": name}, base_path=os.path.dirname(target_path))
            data_dir = pkg.base_path + "/data"
            os.makedirs(data_dir, exist_ok=True)
            func(pkg, *positional_sources_pkgs, **named_sources_pkgs)
            pkg_dump(pkg, target_path)

        named_sources_sources = tuple(named_sources.values())
        target_pkg_file = get_pkg_file(target_pkg_path)
        sources_pkg_files = [
            get_pkg_file(s) for s in positional_sources + named_sources_sources
        ]

        self._env.Command(target_pkg_file, sources_pkg_files, wrapped_func)

        return target_pkg_file
