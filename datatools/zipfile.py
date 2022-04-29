from zipfile import ZipFile


def unzip_file(zipfile, target_path):
    with ZipFile(zipfile, mode="r") as zfile:
        for name in zfile.namelist():
            zfile.extract(name, path=target_path)
            yield name
