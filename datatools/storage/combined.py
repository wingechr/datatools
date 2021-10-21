import os
from .files import FileSystemStorage
from .metadata import SqliteMetadataStorage


class CombinedLocalStorage:

    DEFAULT_DATA_DIR = ".cache"

    def __init__(self, data_dir=None, default_user=None):
        data_dir = data_dir or self.DEFAULT_DATA_DIR
        data_dir_files = os.path.join(data_dir, "files")
        database = os.path.join(data_dir, "metadata.sqlite3")
        self.files = FileSystemStorage(data_dir=data_dir_files)
        self.metadata = SqliteMetadataStorage(
            database=database, default_user=default_user
        )

    def __enter__(self):
        self.files.__enter__()
        self.metadata.__enter__()
        return self

    def __exit__(self, *args):
        self.metadata.__exit__(*args)
        self.files.__exit__(*args)
