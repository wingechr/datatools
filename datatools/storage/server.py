"""TODO"""

from fastapi import FastAPI

from ..utils import parse_cmd_vals
from .types import DataStorage

# FIXME: wrap some errors into HTTP responses


def make_server(data_storage: DataStorage) -> FastAPI:
    """TODO"""
    app = FastAPI()

    @app.get("/info")
    def info():
        return data_storage.info()

    @app.get("/")
    def find(q: list[str]):
        filters_dict = parse_cmd_vals(q)
        return data_storage.list(**filters_dict)

    @app.head("/{uid}")
    def has(uid: str):
        # fixme 200 or 404
        return uid in data_storage

    @app.delete("/{uid}")
    def delete(uid: str):
        del data_storage[uid]

    @app.get("/{uid}")
    def get(uid: str):
        return data_storage[uid]

    @app.put("/{uid}")
    def put(uid: str):
        data_storage[uid] = b""

    @app.get("/{uid}/metadata/")
    def metadata_get(uid, a: str):
        return data_storage.metadata(uid)[a]

    @app.post("/{uid}/metadata/")
    def metadata_set(uid, data: dict):
        with data_storage.metadata(uid) as metadata:
            for k, v in data.items():
                metadata[k] = v

    return app
