import os
import tempfile


class NamedClosedTemporaryFile:
    def __init__(self, suffix=None, prefix=None, dir=None):
        self.suffix = suffix
        self.prefix = prefix
        self.dir = dir
        self.filepath = None

    def __enter__(self):
        file = tempfile.NamedTemporaryFile(
            dir=self.dir, suffix=self.suffix, prefix=self.prefix, delete=False
        )
        self.filepath = file.name
        file.close()
        assert os.path.isfile(self.filepath)
        return self.filepath

    def __exit__(self, *args):
        os.remove(self.filepath)
