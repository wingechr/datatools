"""TODO"""

import functools

from fastapi import Body, FastAPI, HTTPException, Query, Response

from datatools.storage.classes import DataStorage
from datatools.types import StorageFileNotFoundError
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
