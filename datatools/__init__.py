__version__ = "0.3.0"
__all__ = [
    "exceptions",
    "constants",
    "utils",
    "resource",
    "storage",
    "cache",
    "Storage",
    "StorageGlobal",
    "StorageEnv",
]

import click

from . import cache, constants, exceptions, loader, resource, storage, utils
from .main import main
from .storage import Storage, StorageEnv, StorageGlobal

# =============================================
# patch scripts with resource
# =============================================

# patch storage class
# storage.StorageAbstractBase.resource = resource.Resource._storage_resource
#
#
## patch main script
# @main.group("res")
# @click.pass_context
# @click.argument("uri")
# @click.option("--name", "-n")
# def __resource(ctx, uri, name: str = None):
#    storage = ctx.obj
#    resource = storage.resource(uri=uri, name=name)
#    ctx.obj = resource
#
#
## patch main script
# @__resource.command("save")
# @click.pass_obj
# def data_put(resource: "resource.Resource"):
#    resource._save_if_not_exist()
#    print(resource.name)
#

# =============================================
# patch scripts with cache
# =============================================

# patch storage class
# storage.StorageAbstractBase.cache = cache.cache


# =============================================
# patch resource with loader
# =============================================


# patch storage class
# TODO: maybe move to resource
# def _load(resource, **kwargs):
#    resource._save_if_not_exist()
#    return loader.load(filepath=resource.filepath, **kwargs)
#
#
# resource.Resource.load = _load
#
