import json
import logging
import os
from contextlib import ExitStack
from pathlib import Path

from filelock import SoftFileLock


class GlobalExitStack:

    _exit_stack = None

    def __enter__(self):
        if not self._exit_stack:
            # logging.debug("entering global context")
            self.__class__._exit_stack = ExitStack().__enter__()
        return self

    def __exit__(self, *exc):
        # logging.debug("exiting global context")
        self._exit_stack.__exit__(*exc)

    @classmethod
    def enter_context(cls, cm):
        return cls._exit_stack.enter_context(cm)


IS_OPEN = "_is_open"
IS_CHANGED = "_is_changed"


def is_open(obj):
    return getattr(obj, IS_OPEN, False)


def set_open(obj, value):
    return setattr(obj, IS_OPEN, value)


def is_changed(obj):
    return getattr(obj, IS_CHANGED, False)


def set_changed(obj, value):
    return setattr(obj, IS_CHANGED, value)


def assert_open(fun):
    def decorated_fun(obj, *args, **kwargs):
        if not is_open(obj):
            exit_stack = GlobalExitStack
            exit_stack.enter_context(obj)
        return fun(obj, *args, **kwargs)

    return decorated_fun


def assert_open_changed(fun):
    def decorated_fun(obj, *args, **kwargs):
        if not is_open(obj):
            exit_stack = GlobalExitStack
            exit_stack.enter_context(obj)
        setattr(obj, IS_CHANGED, True)
        return fun(obj, *args, **kwargs)

    return decorated_fun


class AbstractResourceContainer:

    _class = None

    def __init__(self):
        self._items = {}

    def _get_instance_id(self, *args, **kwargs):
        raise NotImplementedError()

    def __call__(self, *args, **kwargs):
        """get/create instance"""
        instance_id = self._get_instance_id(*args, **kwargs)
        logging.debug(f"Instance Id: {instance_id}")
        if instance_id not in self._items:
            # create instance
            instance = self._class(*args, **kwargs)
            self._items[instance_id] = instance

        return self._items[instance_id]


class ByteResource:
    def __init__(self, path):
        self.path = self.get_realpath(path)
        self._lock = None
        self._data = None

    @property
    def _lock_path(self):
        return self.path + ".lock"

    @classmethod
    def get_realpath(cls, path):
        return str(Path(path).resolve())

    @classmethod
    def get_uri(cls, path):
        return Path(path).resolve().as_uri()

    def __enter__(self):
        # logging.debug(f"__enter__: {self}")
        if is_open(self):
            return self
        # logging.debug(f"OPENING: {self}")
        os.makedirs(os.path.dirname(self._lock_path), exist_ok=True)
        self._lock = SoftFileLock(self._lock_path).__enter__()
        self._data = self._read()
        set_open(self, True)
        set_changed(self, False)
        return self

    def __exit__(self, *exc):
        # logging.debug(f"__exit__: {self}")
        if not is_open(self):
            return
        # logging.debug(f"CLOSING: {self}, errors={any(exc)}, changed={is_changed(self)}")  # noqa
        if not any(exc) and is_changed(self):
            self._write(self._data)
        self._lock.__exit__(*exc)
        set_changed(self, False)

    def _read(self):
        if not os.path.isfile(self.path):
            return b""
        logging.debug(f"READING from {self.path}")
        with open(self.path, "rb") as file:
            return file.read()

    # @assert_open
    def _write(self, data):
        assert is_open(self)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        logging.debug(f"WRITING to {self.path}")
        with open(self.path, "wb") as file:
            file.write(data)


class TextResource(ByteResource):
    encoding = "utf-8"

    def _read(self):
        return super()._read().decode(self.encoding)

    def _write(self, data):
        super()._write(data.encode(self.encoding))


class JsonResource(TextResource):
    indent = 2
    sort_keys = False
    ensure_ascii = False

    def _read(self):
        if not os.path.isfile(self.path):
            return {}
        return json.loads(super()._read())

    def _write(self, data):
        super()._write(
            json.dumps(
                data,
                indent=self.indent,
                sort_keys=self.sort_keys,
                ensure_ascii=self.ensure_ascii,
            )
        )

    @assert_open_changed
    def f(self, k, v):
        self._data[k] = v


class JsonResourceContainer(AbstractResourceContainer):
    _class = JsonResource

    def _get_instance_id(self, path, *args, **kwargs):
        # FIXME: lower(), because in windows,its case insensitive.
        # but what about unix?
        return self._class.get_uri(path).lower()


logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


def example():

    jsons = JsonResourceContainer()

    with GlobalExitStack():
        with jsons("test.json") as j1:

            j1.f("a", 1)

            with j1:
                jsons("test.json").f("a", 4)
