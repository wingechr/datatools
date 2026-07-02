"""TODO"""

from collections.abc import Iterable
import functools
import re

from fastapi import Body, FastAPI, HTTPException, Query, Response
import httpx

from datatools.exceptions import StorageException, StorageFileNotFoundError
from datatools.storage.base import DataStorage, MetadataStorage
from datatools.types import HTTP_METHOD, MetadataAttribute, MetadataValue, Name
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

    @app.head("/data/{name:path}")
    @catch_exceptions
    def has(name: str):
        if name not in data_storage:
            raise StorageFileNotFoundError(name)

    @app.delete("/data/{name:path}")
    @catch_exceptions
    def delete(name: str):
        del data_storage[name]

    @app.get("/data/{name:path}")
    @catch_exceptions
    def get(name: str):
        data = data_storage[name]
        return Response(content=data)

    @app.put("/data/{name:path}")
    @catch_exceptions
    def put(name: str, data: bytes = Body(...)):
        data_storage[name] = data

    @app.get("/metadata/{name:path}")
    @catch_exceptions
    def metadata_get(name, a: str):
        return data_storage.metadata(name)[a]

    @app.post("/metadata/{name:path}")
    @catch_exceptions
    def metadata_set(name, data: dict):
        metadata = data_storage.metadata(name)
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

    def _contains(self, name: Name) -> bool:
        try:
            self._request(path=f"/data/{name}", method="HEAD")
            return True
        except StorageFileNotFoundError:
            return False

    def _getitem(self, name: Name) -> bytes:
        resp = self._request(path=f"/data/{name}", method="GET")
        return resp.content

    def _setitem(self, name: Name, data: bytes) -> None:
        self._request(path=f"/data/{name}", method="PUT", data=data)

    def _delitem(self, name: Name) -> None:
        self._request(path=f"/data/{name}", method="DELETE")

    def _list(self) -> Iterable[Name]:
        return self._request(path="/data").json()

    def _find(self, **filters: MetadataValue) -> Iterable[Name]:
        filters_list = [f"{k}={v}" for k, v in filters.items()]
        return self._request(path="/data", params={"q": filters_list}).json()

    def _metadata(self, name: Name) -> HttpMetadataStorage:
        url = self._location + f"/metadata/{name}"
        return HttpMetadataStorage(url)

    def info(self) -> dict:
        """TODO"""
        info_remote = self._request(path="/info").json()
        info_client = super().info()
        info_client.update({"remote": info_remote})

        return info_client
