from .utils.cache import FileCache

"""
schema can be given as
    * object
    * filepath to local file
    * uri
"""


class SchemaFileCache(FileCache):
    def _get_path_exists(self, id):
        pass
