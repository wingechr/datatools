"""TODO"""

from collections.abc import Iterable
import functools
import re
from typing import Literal

from fastapi import Body, FastAPI, HTTPException, Query, Response
import httpx

from datatools.exceptions import StorageFileNotFoundError
from datatools.storage.base import DataStorage, MetadataStorage
from datatools.types import UID, MetadataAttribute, MetadataValue
from datatools.utils import parse_cmd_vals


def catch_StorageFileNotFoundError(fun):
    """on StorageFileNotFoundError, return 404"""

    @functools.wraps(fun)
    def _fun(*args, **kwargs):
        try:
            return fun(*args, **kwargs)
        except StorageFileNotFoundError as err:
            raise HTTPException(status_code=404) from err

    return _fun


def make_server_app(data_storage: DataStorage) -> FastAPI:
    """TODO"""
    app = FastAPI()

    @app.get("/info")
    def info():
        return data_storage.info()

    @app.get("/")
    def find(q: list[str] = Query(default=[])):  # noqa: B008
        filters_dict = parse_cmd_vals(q)
        return data_storage.find(**filters_dict)

    @app.head("/{uid}")
    def has(uid: str):
        if uid not in data_storage:
            raise HTTPException(status_code=404)

    @app.delete("/{uid}")
    @catch_StorageFileNotFoundError
    def delete(uid: str):
        del data_storage[uid]

    @app.get("/{uid}")
    @catch_StorageFileNotFoundError
    def get(uid: str):
        data = data_storage[uid]
        return Response(content=data)

    @app.put("/{uid}")
    @catch_StorageFileNotFoundError
    def put(uid: str, data: bytes = Body(...)):
        data_storage[uid] = data

    @app.get("/{uid}/metadata/")
    def metadata_get(uid, a: str):
        return data_storage.metadata(uid)[a]

    @app.post("/{uid}/metadata/")
    def metadata_set(uid, data: dict):
        metadata = data_storage.metadata(uid)
        for k, v in data.items():
            metadata[k] = v

    return app


class HttpMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, url: str):
        self._location = url

    def _request(
        self,
        path: str = "/",
        method: Literal["GET", "PUT", "POST", "DELETE", "HEAD", "PATCH"] = "GET",
        params: dict | None = None,
        data: dict | None = None,
    ):
        url = self._location + path
        resp = httpx.request(method=method, url=url, params=params, json=data)
        resp.raise_for_status()
        return resp

    def _getitem(self, attribute: MetadataAttribute) -> Iterable[MetadataValue]:
        return self._request(params={"a": attribute}).json()

    def _setitem(self, attribute: MetadataAttribute, value: MetadataValue) -> None:
        self._request(method="POST", data={attribute: value})


class HttpDataStorage(DataStorage):
    """TODO"""

    is_delegating = True  # delegates to http server

    @classmethod
    def _can_handle(cls, location: str) -> bool:
        return bool(re.match(r"^https?://", location))

    def _request(
        self,
        path: str = "/",
        method: Literal["GET", "PUT", "POST", "DELETE", "HEAD", "PATCH"] = "GET",
        params: dict | None = None,
        data: bytes | None = None,
    ):
        url = self._location + path
        resp = httpx.request(method=method, url=url, params=params, content=data)
        resp.raise_for_status()
        return resp

    def _contains(self, uid: UID) -> bool:
        try:
            resp = self._request(path=f"/{uid}", method="HEAD")
            return resp.is_success
        except httpx.HTTPStatusError as exc:
            if not exc.response.status_code == 404:
                raise
        return False

    def _getitem(self, uid: UID) -> bytes:
        resp = self._request(path=f"/{uid}", method="GET")
        return resp.content

    def _setitem(self, uid: UID, data: bytes) -> None:
        self._request(path=f"/{uid}", method="PUT", data=data)

    def _delitem(self, uid: UID) -> None:
        self._request(path=f"/{uid}", method="DELETE")

    def _list(self) -> Iterable[UID]:
        return self._request(path="/").json()

    def _find(self, **filters: MetadataValue) -> Iterable[UID]:
        filters_list = [f"{k}={v}" for k, v in filters.items()]
        return self._request(path="/", params={"q": filters_list}).json()

    def _metadata(self, uid: UID) -> HttpMetadataStorage:
        url = self._location + f"/{uid}/metadata"
        return HttpMetadataStorage(url)

    def info(self) -> dict:
        """TODO"""
        info_remote = self._request(path="/info").json()
        info_client = super().info()
        info_client.update({"remote": info_remote})

        return info_client
