import datetime
import getpass
import hashlib
import json
import logging
import os
import re
import shutil
import socket
import tempfile
import urllib.parse
from stat import S_IREAD, S_IRGRP, S_IROTH

import appdirs
import click
import coloredlogs
import filelock
import requests
import tzlocal

from . import __version__

APP_NAME = "datatools"
DATETIMETZ_FMT = "%Y-%m-%d %H:%M:%S%z"


def make_readonly(filepath):
    os.chmod(filepath, S_IREAD | S_IRGRP | S_IROTH)


def get_hash(filepath, method="sha256"):
    hasher = getattr(hashlib, method)()
    with open(filepath, "rb") as file:
        hasher.update(file.read())
    result = {}
    result[method] = hasher.hexdigest()
    return result


class DataIndex:
    def __init__(self, base_dir):
        self._base_dir = os.path.abspath(base_dir)
        self._data_dir = os.path.join(self._base_dir, "data")
        self._index_json = os.path.join(self._base_dir, "datapackage.json")
        self._index_json_lock = filelock.FileLock(self._index_json + ".lock")
        self._data = None
        self._encoding = "utf-8"
        self._changed = None

    def __enter__(self):
        os.makedirs(self._base_dir, exist_ok=True)
        os.makedirs(self._data_dir, exist_ok=True)
        if not os.path.exists(self._index_json):
            logging.info(f"initializing index in : {self._base_dir}")
            self._data = {"resources": []}
            self._write_index()
        self._index_json_lock.__enter__()
        self._read_index()

        return self

    def __exit__(self, exc_type, exc_value, tb):
        if self._changed and not exc_value:
            # write if there was no error
            self._write_index()
        self._index_json_lock.__exit__(exc_type, exc_value, tb)

    def _write_index(self):
        data_s = json.dumps(self._data, ensure_ascii=False, indent=2)
        data_b = data_s.encode(encoding=self._encoding)
        logging.debug(f"writing index: {self._index_json}")
        with open(self._index_json, "wb") as file:
            file.write(data_b)
        self._changed = False

    def _read_index(self):
        logging.debug(f"reading index: {self._index_json}")
        with open(self._index_json, encoding=self._encoding) as file:
            self._data = json.load(file)
        self._changed = False

    def get_resource_id(self, abspath):
        assert abspath
        abspath = os.path.abspath(abspath)
        relpath = os.path.relpath(abspath, self._data_dir)
        relpath = relpath.replace("\\", "/")
        return relpath

    def contains_resource(self, resource_id):
        abs_path = self.get_abs_path(resource_id)
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        return os.path.exists(abs_path)

    def get_abs_path(self, resource_id):
        resource_id = resource_id.lstrip("/")
        assert resource_id
        return os.path.abspath(os.path.join(self._data_dir, resource_id))

    def find_resource_ids_in_repo(self):
        resource_ids = set()
        for rt, _, fs in os.walk(self._data_dir):
            for f in fs:
                abspath = os.path.join(rt, f)
                resource_id = self.get_resource_id(abspath)
                if resource_id in resource_ids:
                    logging.warning(f"duplicate path: {resource_id}")
                    continue
                resource_ids.add(resource_id)
        return resource_ids

    def find_resource_ids_in_index(self):
        resource_ids = set()
        for res in self._data["resources"]:
            resource_id = res.get("path")
            if not resource_id:
                logging.warning(f"resource without path: {res}")
                continue
            if resource_id in resource_ids:
                logging.warning(f"duplicate path: {resource_id}")
                continue
            resource_ids.add(resource_id)

        return resource_ids

    def remove(self, resource_id):
        # TODO: duplicates?
        idx = None
        for i, res in enumerate(self._data["resources"]):
            if res.get("path") == resource_id:
                idx = i
                break

        if idx is None:
            raise KeyError(resource_id)

        assert self._data["resources"][idx]["path"] == resource_id
        del self._data["resources"][idx]

        self._changed = True

    def update(self, abspath, source):
        resource_id = self.get_resource_id(abspath)
        resource = self._get_metadata(abspath, source)
        resource["path"] = resource_id

        # TODO
        res2idx = self.find_resource_ids_in_index()
        if resource_id in res2idx:
            logging.info(f"overwriting metadata for {resource_id}")
            assert self._data["resources"][res2idx[resource_id]]["path"] == resource_id
            del self._data["resources"][res2idx[resource_id]]
            self._changed = True

        self._data["resources"].append(resource)
        self._changed = True

    def _get_metadata(self, abspath, source):
        return {
            "hash": get_hash(abspath),
            "download": {
                "datetime": get_now().strftime(DATETIMETZ_FMT),
                "user": get_user_long(),
                "source": source,
            },
        }

    def download(self, uri, force=False):
        def get_temp_path(suffix=""):
            """do not create the file"""
            tf = tempfile.mktemp(prefix=f"{APP_NAME}_", suffix=suffix)
            return tf

        handler = get_handler(uri)
        resource_id = handler.get_local_rel_path(uri)
        filepath = self.get_abs_path(resource_id)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        if os.path.exists(filepath):
            if not force:
                logging.info(f"exists: {resource_id}")
                return
            else:
                logging.warning(f"overwriting exists: {resource_id}")

        tmp_path = get_temp_path()

        handler.handle(uri, tmp_path)
        shutil.move(tmp_path, filepath)
        make_readonly(filepath)

        self.update(filepath, source=uri)

    def check(self, fix, delete, hash):
        files = self.find_resource_ids_in_repo()
        resources = self.find_resource_ids_in_index()

        # find files that are not in index
        for f in files - resources:
            if fix:
                logging.info(f"Adding: {f}")
                path = self.get_abs_path(f)
                self.update(path, source=None)
            else:
                logging.warning(f"File not in index: {f}")

        # find resources that are not in directory
        for f in resources - files:
            if fix and delete:
                logging.info(f"removing {f}")
                self.remove(f)
            else:
                logging.warning(f"File does not exist: {f}")

        # TODO fix duplicates

        # TODO: check hash
        if hash:
            for res in self._data["resources"]:
                assert res["path"] in files
                path = self.get_abs_path(res["path"])
                index_hash = res.get("hash")
                if index_hash:
                    for method, hashsum in index_hash.items():
                        digest = get_hash(path, method)[method]
                        if digest != hashsum:
                            if fix:
                                logging.warning(
                                    f"Fixing Wrong {method} hashsum for {path}: "
                                    f"{digest}, expected {hashsum}"
                                )
                                res["hash"][method] = get_hash(path, method)[method]
                                self._changed = True
                                # TODO
                            else:
                                logging.warning(
                                    f"Wrong {method} hashsum for {path}: "
                                    f"{digest}, expected {hashsum}"
                                )
                else:  # no hash
                    if fix:
                        logging.warning(f"Fixing No hashsum for {path}")
                        res["hash"] = get_hash(path, method)
                        self._changed = True
                    else:
                        logging.warning(f"No hashsum for {path}")

        # readonly
        for f in files:
            path = self.get_abs_path(f)
            make_readonly(path)


class LoaderHttp:
    def get_local_rel_path(self, uri) -> str:
        url = urllib.parse.urlsplit(uri)
        url.path.split("/")

        host = url.hostname or "localhost"
        path = url.path

        # TODO: maybe urldecode spaces? but not all special chars?

        if not path.startswith("/"):
            path = "/" + path

        if path == "/":
            path = "/index.html"

        path = host + path

        if url.fragment:
            path += "#" + url.fragment

        return path

    @staticmethod
    def can_handle_scheme(scheme) -> bool:
        return scheme.lower() in ("http", "https")

    def handle(self, uri, filepath):
        res = requests.get(uri)
        res.raise_for_status()
        with open(filepath, "wb") as file:
            file.write(res.content)


def get_handler(uri):
    def get_scheme(uri):
        scheme = urllib.parse.urlsplit(uri).scheme
        if not scheme:
            scheme = "file"
        return scheme

    scheme = get_scheme(uri)

    for handler_cls in [LoaderHttp]:
        if handler_cls.can_handle_scheme(scheme):
            return handler_cls()
    raise NotImplementedError(scheme)


def get_now():
    tz_local = tzlocal.get_localzone()
    now = datetime.datetime.now()
    now_tz = now.replace(tzinfo=tz_local)
    return now_tz


def get_user_long():
    def get_user():
        """Return current user name"""
        return getpass.getuser()

    def get_host():
        """Return current domain name"""
        # return socket.gethostname()
        return socket.getfqdn()

    return f"{get_user()}@{get_host()}"


@click.group("main")
@click.pass_context
@click.version_option(__version__)
@click.option(
    "--loglevel",
    "-l",
    type=click.Choice(["debug", "info", "warning", "error"]),
    default="info",
)
@click.option("is_global", "--global", "-g", is_flag=True, help="use global repository")
@click.option("--data-location", "-d", help="change the default location")
def main(ctx, loglevel, is_global, data_location):
    """Script entry point."""

    # setup logging
    if isinstance(loglevel, str):
        loglevel = getattr(logging, loglevel.upper())
    coloredlogs.DEFAULT_LOG_FORMAT = "[%(asctime)s %(levelname)7s] %(message)s"
    coloredlogs.DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    coloredlogs.DEFAULT_FIELD_STYLES = {"asctime": {"color": None}}
    coloredlogs.install(level=loglevel)

    def get_data_location(is_global):
        if is_global:
            path = appdirs.user_data_dir(
                APP_NAME, appauthor=None, version=None, roaming=False
            )
            path += "/data"
        else:
            path = "."
        return path

    data_location = data_location or get_data_location(is_global)
    data_location = os.path.abspath(data_location)
    os.makedirs(data_location, exist_ok=True)

    ctx.obj = ctx.with_resource(DataIndex(data_location))


@main.command("list")
@click.option("regexp", "-r", help="regexp pattern")
@click.pass_obj
def list(index: DataIndex, regexp):
    regexp = re.compile(regexp or ".*")

    for res in index._data["resources"]:
        if regexp.match(res["path"]):
            res = json.dumps(res, indent=2)
            print(res)


@main.command("check")
@click.pass_obj
@click.option("--fix", "-f", is_flag=True, help="fix problems")
@click.option("--delete", "-d", is_flag=True, help="delete index entries for missing")
@click.option("--hash", "-h", is_flag=True, help="check hashes")
def check(index: DataIndex, fix, delete, hash):
    index.check(fix, delete, hash)


@main.command("download")
@click.pass_obj
@click.option("--force", "-f", is_flag=True, help="overwrite existing")
@click.argument("uri")
def download(index: DataIndex, uri, force):
    index.download(uri, force=force)


if __name__ == "__main__":
    main(prog_name=APP_NAME)
