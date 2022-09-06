from typing import List

import pyodbc
import sqlalchemy as sa

dialects = ["sqlite", "postgresql", "mysql", "mssql"]


def odbc_drivers() -> List[str]:
    return pyodbc.drivers()


def guess_odbc_driver(name):
    for dr in odbc_drivers():
        if name.lower() in dr.lower():
            return name.lower()
    raise ValueError(name)


def get_odbc_connectionstring(**kwargs):
    constr = ";".join("%s=%s" % (k, v) for k, v in kwargs.items() if v is not None)
    return constr


def get_uri(dialect, path, driver=None):
    if driver:
        dialect = f"{dialect}+{driver}"
    uri = f"{dialect}://{path}"
    return uri


def get_uri_odbc(dialect, odbc_driver=None, **obbc_kwargs):
    cs = get_odbc_connectionstring(driver=odbc_driver, **obbc_kwargs)
    path = f"/?odbc_connect={cs}"
    return get_uri(dialect, path, driver="pyodbc")


def get_uri_odbc_access(database):
    odbc_driver = guess_odbc_driver("microsoft access")
    return get_uri_odbc("access", odbc_driver=odbc_driver, dbq=database)


def get_uri_odbc_sqlite(database):
    odbc_driver = guess_odbc_driver("sqlite3")
    return get_uri_odbc("sqlite", odbc_driver=odbc_driver, database=database)


def get_uri_odbc_sqlserver(server, database=None):
    odbc_driver = guess_odbc_driver("sql server")
    return get_uri_odbc(
        "mssql", odbc_driver=odbc_driver, server=server, database=database
    )


def get_uri_sqlite(database=None):
    if not database:  # memory
        path = ""
    else:
        path = database
        if not path.startswith("/"):
            path = "/" + path
    return get_uri(dialect="sqlite", path=path)


def create_mock_engine(dialect_name, executor):
    dialect = sa.create_engine(dialect_name + "://").dialect

    def _executor(sql, *args, **kwargs):
        executor(str(sql.compile(dialect=dialect)))

    return sa.create_mock_engine(dialect_name + "://", _executor)
