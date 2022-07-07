import os

from .files import FileSystemStorage
from .metadata import SqliteMetadataStorage
from .utils import path2file_uri


class AbstractCombinedLocalStorage:
    def __init__(self, file_storage, metadata_storage):
        self._files = file_storage
        self._metadata = metadata_storage

    def __enter__(self):
        self._files.__enter__()
        self._metadata.__enter__()
        return self

    def __exit__(self, *args):
        self._metadata.__exit__(*args)
        self._files.__exit__(*args)

    def __contains__(self, file_id):
        return file_id in self._files

    def get_file(self, file_id, check_integrity=False):
        return self._files.get_file(file_id, check_integrity=check_integrity)

    def set_file(self, data_stream):
        return self._files.set_file(data_stream)

    def set_metadata(self, file_id, identifier_values, user=None, timestamp_utc=None):
        return self._metadata.set_metadata(
            file_id, identifier_values, user=user, timestamp_utc=timestamp_utc
        )

    def get_metadata(self, file_id, identifier):
        return self._metadata.get_metadata(file_id, identifier)

    def get_all_metadata(self, file_id):
        return self._metadata.get_all_metadata(file_id)

    def get_all_metadata_extended(self, file_id):
        return self._metadata.get_all_extended(file_id)

    def set_file_by_path(self, file_path):
        """Add a local file by path

        This also adds the name/path as metadata
        """
        file_path = os.path.abspath(file_path)
        file_name = os.path.basename(file_path)
        _, file_extension = os.path.splitext(file_name)
        file_extension = file_extension.lstrip(".")
        file_uri = path2file_uri(file_path)

        with open(file_path, "rb") as file:
            file_id = self.set_file(file)

        self.set_metadata(
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
