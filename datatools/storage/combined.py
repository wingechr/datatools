import os
from .files import FileSystemStorage
from .metadata import SqliteMetadataStorage
from datatools.utils import path2file_uri


class AbstractCombinedLocalStorage:
    def __init__(self, file_storage, metadata_storage):
        self.files = file_storage
        self.metadata = metadata_storage

    def __enter__(self):
        self.files.__enter__()
        self.metadata.__enter__()
        return self

    def __exit__(self, *args):
        self.metadata.__exit__(*args)
        self.files.__exit__(*args)

    def add_file(self, file_path):
        """Add a local file by path

        This also adds the name/path as metadata
        """
        file_path = os.path.abspath(file_path)
        file_name = os.path.basename(file_path)
        _, file_extension = os.path.splitext(file_name)
        file_extension = file_extension.lstrip(".")
        file_uri = path2file_uri(file_path)

        with open(file_path, "rb") as file:
            file_id = self.files.set(file)

        self.metadata.set(
            file_id,
            {
                "file_name": file_name,
                "file_extension": file_extension,
                "file_uri": file_uri,
                "file_path": file_path,
            },
        )

        return file_id


class CombinedLocalStorage(AbstractCombinedLocalStorage):

    DEFAULT_DATA_DIR = ".cache"

    def __init__(self, data_dir=None, default_user=None):
        data_dir = data_dir or self.DEFAULT_DATA_DIR
        data_dir_files = os.path.join(data_dir, "files")
        database = os.path.join(data_dir, "metadata.sqlite3")
        super().__init__(
            file_storage=FileSystemStorage(data_dir=data_dir_files),
            metadata_storage=SqliteMetadataStorage(
                database=database, default_user=default_user
            ),
        )
