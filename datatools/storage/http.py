"""TODO"""

from collections.abc import Iterable
import functools
import re

from fastapi import Body, FastAPI, HTTPException, Query, Response
import httpx

from datatools.exceptions import StorageException, StorageFileNotFoundError
from datatools.storage.base import DataStorage, MetadataStorage
from datatools.types import HTTP_METHOD, UID, MetadataAttribute, MetadataValue
from datatools.utils import parse_cmd_vals


def catch_exceptions(fun):
    """on StorageFileNotFoundError, return 404"""

    @functools.wraps(fun)
    def _fun(*args, **kwargs):
        try:
            return fun(*args, **kwargs)
        except StorageFileNotFoundError as err:
            raise HTTPException(status_code=404, detail=str(err)) from err
        except StorageException as err:
            # return info of exception
            raise HTTPException(status_code=400, detail=str(err)) from err

    return _fun


def make_server_app(data_storage: DataStorage) -> FastAPI:
    """TODO"""
    app = FastAPI()

    @app.get("/info")
    @catch_exceptions
    def info():
        return data_storage.info()

    @app.get("/data")
    @catch_exceptions
    def find(q: list[str] = Query(default=[])):  # noqa: B008
        filters_dict = parse_cmd_vals(q)
        return data_storage.find(**filters_dict)

    @app.head("/data/{uid:path}")
    @catch_exceptions
    def has(uid: str):
        if uid not in data_storage:
            raise StorageFileNotFoundError(uid)

    @app.delete("/data/{uid:path}")
    @catch_exceptions
    def delete(uid: str):
        del data_storage[uid]

    @app.get("/data/{uid:path}")
    @catch_exceptions
    def get(uid: str):
        data = data_storage[uid]
        return Response(content=data)

    @app.put("/data/{uid:path}")
    @catch_exceptions
    def put(uid: str, data: bytes = Body(...)):
        data_storage[uid] = data

    @app.get("/metadata/{uid:path}")
    @catch_exceptions
    def metadata_get(uid, a: str):
        return data_storage.metadata(uid)[a]

    @app.post("/metadata/{uid:path}")
    @catch_exceptions
    def metadata_set(uid, data: dict):
        metadata = data_storage.metadata(uid)
        for k, v in data.items():
            metadata[k] = v

    return app


class HttpMetadataStorage(MetadataStorage):
    """TODO"""

    def __init__(self, url: str):
        self._url = url

    def _request(
        self,
        path: str = "/",
        method: HTTP_METHOD = "GET",
        params: dict | None = None,
        data: dict | None = None,
    ):
        url = self._url
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
        method: HTTP_METHOD = "GET",
        params: dict | None = None,
        data: bytes | None = None,
    ):
        url = self._location + path
        resp = httpx.request(method=method, url=url, params=params, content=data)

        if resp.is_error:
            if resp.status_code == 404:
                raise StorageFileNotFoundError()
            elif resp.status_code < 500:
                raise StorageException(resp.content)
            else:
                raise Exception(resp.content)  # pragma: no cover

        return resp

    def _contains(self, uid: UID) -> bool:
        try:
            self._request(path=f"/data/{uid}", method="HEAD")
            return True
        except StorageFileNotFoundError:
            return False

    def _getitem(self, uid: UID) -> bytes:
        resp = self._request(path=f"/data/{uid}", method="GET")
        return resp.content

    def _setitem(self, uid: UID, data: bytes) -> None:
        self._request(path=f"/data/{uid}", method="PUT", data=data)

    def _delitem(self, uid: UID) -> None:
        self._request(path=f"/data/{uid}", method="DELETE")

    def _list(self) -> Iterable[UID]:
        return self._request(path="/data").json()

    def _find(self, **filters: MetadataValue) -> Iterable[UID]:
        filters_list = [f"{k}={v}" for k, v in filters.items()]
        return self._request(path="/data", params={"q": filters_list}).json()

    def _metadata(self, uid: UID) -> HttpMetadataStorage:
        url = self._location + f"/metadata/{uid}"
        return HttpMetadataStorage(url)

    def info(self) -> dict:
        """TODO"""
        info_remote = self._request(path="/info").json()
        info_client = super().info()
        info_client.update({"remote": info_remote})

        return info_client
