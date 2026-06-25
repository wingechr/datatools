"""TODO"""

from fastapi import Body, FastAPI, HTTPException, Query, Response

from datatools.storage.classes import DataStorage
from datatools.types import (
    StorageFileNotFoundError,
)
from datatools.utils import parse_cmd_vals

# FIXME: wrap some errors into HTTP responses


def make_server_app(data_storage: DataStorage) -> FastAPI:
    """TODO"""
    app = FastAPI()

    @app.get("/info")
    def info():
        return data_storage.info()

    @app.get("/")
    def find(q: list[str] = Query(default=[])):  # noqa: B008
        filters_dict = parse_cmd_vals(q)
        return data_storage.list(**filters_dict)

    @app.head("/{uid}")
    def has(uid: str):
        if uid not in data_storage:
            raise HTTPException(status_code=404)

    @app.delete("/{uid}")
    def delete(uid: str):
        try:
            del data_storage[uid]
        except StorageFileNotFoundError as err:
            raise HTTPException(status_code=404) from err

    @app.get("/{uid}")
    def get(uid: str):
        try:
            data = data_storage[uid]
            return Response(content=data)
        except StorageFileNotFoundError as err:
            raise HTTPException(status_code=404) from err

    @app.put("/{uid}")
    def put(uid: str, data: bytes = Body(...)):
        try:
            data_storage[uid] = data
        except StorageFileNotFoundError as err:
            raise HTTPException(status_code=403) from err

    @app.get("/{uid}/metadata/")
    def metadata_get(uid, a: str):
        return data_storage.metadata(uid)[a]

    @app.post("/{uid}/metadata/")
    def metadata_set(uid, data: dict):
        metadata = data_storage.metadata(uid)
        for k, v in data.items():
            metadata[k] = v

    return app
