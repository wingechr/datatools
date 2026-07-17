"""TODO"""

from collections.abc import Callable

from datatools.io import JsonIO
from datatools.process.task import string_get_hash_data
from datatools.storage import FileDataStorage
from datatools.types import StrPath
from datatools.utils import sanitize_filename


def simple_json_cache(location: StrPath = "__data__") -> Callable:
    """TODO"""
    st = FileDataStorage(location)
    return st.cache(
        output_write_byte_data=JsonIO.dump,
        output_from_bytes=JsonIO.load,
        get_name_from_hash=lambda x: f"{sanitize_filename(x)}.json",
        get_task_id=string_get_hash_data,
    )
