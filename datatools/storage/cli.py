"""TODO"""

from collections.abc import Callable, Iterable
import json
import logging
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from datatools.exceptions import (
    SubprocessStatus,
)
from datatools.storage.base import DataStorage, MetadataStorage
from datatools.types import MetadataAttribute, MetadataValue, Name
from datatools.utils import json_dumps_for_print, reverse_prints, try_parse_json_str


class TestCliMetadataDataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, name: Name, request: Callable):
        self._name = name
        self._request = request

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        data = self._request("metadata", "get", self._name, str(attribute))
        return try_parse_json_str(data)

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        self._request(
            "metadata",
            "set",
            self._name,
            f"{attribute}={json_dumps_for_print(value)}",
        )


class CliWrapperDataStorage(DataStorage):
    """TODO"""

    is_delegating = True  # delegates to script

    def __init__(self, location: Any = None):
        self._location = location
        self._script = str(Path(__file__).parent / "__main__.py")
        self._clirunner = CliRunner()

        from datatools.storage.__main__ import main as storage_main_cli

        self._storage_main_cli = storage_main_cli

    def _request(self, *args: str, data: bytes | None = None) -> bytes:
        cmd = ["-l", str(self._location)] + list(args)
        # stdout, _stderr = call_script(
        #    self._script, cmd, data
        # )
        logging.debug("CLI " + " ".join(cmd))
        result = self._clirunner.invoke(self._storage_main_cli, cmd, input=data)
        if result.exit_code:
            raise SubprocessStatus(result.exit_code)

        stdout = result.stdout_bytes

        return stdout

    def _contains(self, name: Name) -> bool:
        try:
            self._request("has", name)
        except SubprocessStatus:
            return False
        return True

    def _getitem(self, name: Name) -> bytes:
        return self._request("get", name)

    def _setitem(self, name: Name, data: bytes) -> None:
        self._request("put", name, data=data)

    def _delitem(self, name: Name) -> None:
        self._request("delete", name)

    def _metadata(self, name: Name) -> MetadataStorage:
        return TestCliMetadataDataStorage(name, self._request)

    def _find(self, **filters: MetadataValue) -> Iterable[Name]:
        filters_str = [f"{k}={v}" for k, v in filters.items()]
        data = self._request("find", *filters_str)
        return reverse_prints(data)

    def _list(self) -> Iterable[Name]:
        return self._find()

    def info(self) -> dict:
        """TODO"""
        info_remote = self._request("info")
        info_remote = json.loads(info_remote)
        info_client = super().info()
        info_client.update({"remote": info_remote})

        return info_client
