from typing import List

import pyodbc


def odbc_drivers() -> List[str]:
    return pyodbc.drivers()


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
    odbc_driver = "microsoft access driver (*.mdb, *.accdb)"
    return get_uri_odbc("access", odbc_driver=odbc_driver, dbq=database)


def get_uri_odbc_sqlite(database):
    odbc_driver = "sqlite3 odbc driver"
    return get_uri_odbc("sqlite", odbc_driver=odbc_driver, database=database)


def get_uri_odbc_sqlserver(server, database=None):
    odbc_driver = "sql server"
    return get_uri_odbc(
        "mssql", odbc_driver=odbc_driver, server=server, database=database
    )


def get_uri_sqlserver(server, database=None):
    path = f"{server}/"
    if database:
        path = path + database
    return get_uri("mssql", path)


def get_uri_sqlite(database=None):
    if not database:  # memory
        path = ""
    else:
        path = database
        if not path.startswith("/"):
            path = "/" + path
    return get_uri(dialect="sqlite", path=path)
