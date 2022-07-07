import logging
import os
import re
from urllib.parse import urlparse

from .datapackage import Package, get_pkg_json_file, pkg_dump, pkg_load
from .files import copy_uri, get_filepath_uri, makedirs, normpath
from .requests import download_file
from .sql import download_sql
from .text import normalize_name
from .zipfile import unzip_all


def validate_resource_path(resource_path):
    if not resource_path.startswith("data/"):
        raise ValueError(resource_path)
    return normpath(resource_path)


def get(target_pkg, source_pkg):
    for resource in source_pkg.resources:
        resource_path = resource.descriptor["data"]["target"]
        resource_path = validate_resource_path(resource_path)
        target_filepath = target_pkg.base_path + "/" + resource_path
        target_dir = os.path.dirname(target_filepath)
        makedirs(target_dir, exist_ok=True)
        source_uri = resource.descriptor["data"]["source"]
        source_scheme = urlparse(source_uri).scheme
        logging.debug(f"SOURCE: {source_uri}")
        if re.match(r"^file$", source_scheme):
            copy_uri(
                source_uri,
                target_filepath,
                overwrite=True,
            )
        elif re.match(r"^https?$", source_scheme):
            download_file(source_uri, target_filepath, overwrite=True)
        elif re.match(r"^.*sql.*$", source_scheme):
            download_sql(source_uri, target_filepath, overwrite=True)
        else:
            raise NotImplementedError(source_scheme)
        target_pkg.add_resource(
            {
                "name": resource.name,
                "source": source_uri,
                "path": normpath(resource_path),
            }
        )


def unzip(target_pkg, source_pkg):
    # TODO: test
    target_data_dir = target_pkg.base_path + "/data"
    for resource in source_pkg.resources:
        source_zipfile = source_pkg.base_path + "/" + resource.descriptor["path"]
        source_sha256 = resource.descriptor["sha256"]
        for filename in unzip_all(source_zipfile, target_data_dir):
            target_pkg.add_resource(
                {
                    "name": normalize_name(filename),
                    "source": get_filepath_uri(source_zipfile),
                    "source_sha256": source_sha256,
                    "path": "data/" + normpath(filename),
                }
            )


class DatapackageBuilder:
    def __init__(self, env):
        self._env = env

    def __call__(self, func, target_pkg_path, *positional_sources, **named_sources):
        def wrapped_func(target, source, env):
            if len(target) != 1:
                raise Exception("Only works with single target")
            target_json_path = env.GetBuildPath(target[0])
            sources_paths = [env.GetBuildPath(p) for p in source]
            sources_pkgs = [pkg_load(s) for s in sources_paths]
            named_sources_names = tuple(named_sources.keys())
            n_positional = len(sources_pkgs) - len(named_sources_names)
            positional_sources_pkgs = sources_pkgs[:n_positional]
            named_sources_pkgs = dict(
                zip(named_sources_names, sources_pkgs[n_positional:])
            )
            basename = os.path.basename(target_json_path)
            if basename != "datapackage.json":
                raise Exception(target_json_path)
            # use folder name
            target_basepath = os.path.dirname(target_json_path)
            target_datapath = target_basepath + "/data"
            pkg_name = os.path.basename(target_basepath)
            pkg_name = normalize_name(pkg_name)
            pkg = Package({"name": pkg_name}, base_path=target_basepath)
            makedirs(target_datapath, exist_ok=True)
            func(pkg, *positional_sources_pkgs, **named_sources_pkgs)
            pkg_dump(pkg, target_json_path)

        named_sources_sources = tuple(named_sources.values())
        target_pkg_json_file = get_pkg_json_file(target_pkg_path)
        target_pkg_basepath = os.path.dirname(target_pkg_json_file)
        sources_pkg_json_files = [
            get_pkg_json_file(s) for s in positional_sources + named_sources_sources
        ]

        tgt = self._env.Command(
            target_pkg_json_file, sources_pkg_json_files, wrapped_func
        )
        self._env.Clean(tgt, target_pkg_basepath)

        return tgt
