import datetime
import json
import logging
import math
import re
from collections import Counter
from decimal import Decimal
from typing import Iterable, List, Tuple, Union

import frictionless
import frictionless as fl
import genson
import jsonschema
import pandas as pd
import sqlalchemy as sa

from . import storage
from .exceptions import SchemaError, ValidationError
from .utils import df_to_values, get_err_message, sa_create_engine


def infer_schema_from_objects(data: list):
    builder = genson.SchemaBuilder()
    builder.add_schema({"type": "object", "properties": {}})
    for item in data:
        builder.add_object(item)
    item_schema = builder.to_schema()
    schema = {"type": "array", "items": item_schema}
    return schema


def validate(data: Union[list, pd.DataFrame], schema: dict) -> None:
    """validate date against schema

    Parameters
    ----------
    data : Union[list,pd.DataFrame]
        data object: either a list of dicts or a DataFrame
    schema : dict
        schema object, either a json schema or a frictionless table schema

    Raises
    ------
    Exception
        _description_
    NotImplementedError
        _description_
    """
    if not schema:
        raise Exception("No schema")

    if isinstance(data, pd.DataFrame):
        data = df_to_values(data)

    if is_jsonschema(schema):
        validator = get_jsonschema_validator(schema)
        validator(data)
    elif is_frictionlessschema(schema):
        resource = {
            "name": "todo",
            "schema": schema,
            "profile": "tabular-data-resource",
            "data": data,
        }
        validate_resource(resource)
    else:
        raise NotImplementedError()


def is_jsonschema(schema: object):
    return "type" in schema


def is_frictionlessschema(schema: object):
    return "fields" in schema


def validate_resource(resource_descriptor):
    try:
        res = frictionless.Resource(resource_descriptor)
    except frictionless.exception.FrictionlessException as exc:
        raise SchemaError(exc)

    rep = res.validate()

    if rep.stats["errors"]:
        errors = []
        for report_task in rep.tasks:
            for err in report_task.errors:
                err_msg = get_err_message(err)
                errors.append(err_msg)
        err_str = "\n".join(errors)
        raise ValidationError(err_str)


def get_jsonschema_storage():
    return storage.StorageGlobal()


def get_jsonschema(schema_url):
    with get_jsonschema_storage().resource(source_uri=schema_url)._open() as file:
        return json.load(file)


def get_jsonschema_validator(schema):
    """Return validator instance for schema.

    Example:

    >>> schema = {"type": "object", "properties": {"id": {"type": "integer"}}, "required": [ "id" ]}  # noqa
    >>> validator = get_jsonschema_validator(schema)
    >>> validator({})
    Traceback (most recent call last):
        ...
    ValueError: 'id' is a required property ...

    >>> validator({"id": "a"})
    Traceback (most recent call last):
        ...
    ValueError: 'a' is not of type 'integer' ...

    >>> validator({"id": 1})

    """

    if isinstance(schema, str):
        schema = get_jsonschema(schema)

    validator_cls = jsonschema.validators.validator_for(schema)
    # check if schema is valid
    try:
        validator_cls.check_schema(schema)
    except Exception as exc:
        raise SchemaError(exc)
    validator = validator_cls(schema)

    def validator_function(instance):
        errors = []
        for err in validator.iter_errors(instance):
            # path in data structure where error occurs
            path = "$" + "/".join(str(x) for x in err.absolute_path)
            errors.append("%s in %s" % (err.message, path))
        if errors:
            err_str = "\n".join(errors)
            # logging.error(err_str)
            raise ValidationError(err_str)

    return validator_function


# -------------------------
# Schema conversion (Work in progress)
# -------------------------


class DataFrame:
    pass


class StopValidation(Exception):
    pass


def is_null(value):
    if value is None:
        return True
    elif isinstance(value, float) and math.isnan(value):
        return True
    return False


def find_duplicates(values) -> dict:
    counter = Counter(values)
    duplicates = dict((key, count) for key, count in counter.items() if count > 1)
    return duplicates


def validate_python_type(value, type_class):
    if not isinstance(value, type_class):
        raise SchemaError(
            f"value {value} should be type {type_class.__name__}, "
            f"but is {type(value).__name__}"
        )


def validate_not_python_type(value, type_class):
    if isinstance(value, type_class):
        raise SchemaError(f"value {value} should not be type {type_class}")


class Type:
    def __init__(self, is_nullable=False, name: str = None):
        self.name = name
        self.is_nullable = is_nullable

    def validate(self, value, parse_from_string: bool = False) -> Tuple[bool, object]:
        if is_null(value):
            if self.is_nullable:  # finish validation
                return True, value
            else:
                raise SchemaError(
                    f"value {value} conflicts with is_nullable={self.is_nullable}"
                )
        elif parse_from_string and isinstance(value, str):
            value = self.from_string(value)
        return False, value

    def from_string(self, value):
        raise NotImplementedError(self.__class__)


class TypeText(Type):
    def validate(self, value, parse_from_string: bool = False):
        finished, value = super().validate(value, parse_from_string=parse_from_string)
        if not finished:
            validate_python_type(value, str)
        return finished, value

    def from_string(self, value):
        return value


class TypeTextMaxLen(TypeText):
    def __init__(self, max_length: int, is_nullable=False, name: str = None):
        super().__init__(is_nullable=is_nullable, name=name)
        self.max_length = int(max_length)

    def validate(self, value, parse_from_string: bool = False):
        finished, value = super().validate(value, parse_from_string=parse_from_string)
        if not finished:
            if len(value) > self.max_length:
                raise SchemaError(
                    f"length of {value} ({len(value)}) > {self.max_length}"
                )
        return finished, value


class TypeTextFixLen(TypeText):
    def __init__(self, length: int, is_nullable=False, name: str = None):
        super().__init__(is_nullable=is_nullable, name=name)
        self.length = int(length)

    def validate(self, value, parse_from_string: bool = False):
        finished, value = super().validate(value, parse_from_string=parse_from_string)
        if not finished:
            if len(value) != self.length:
                raise SchemaError(f"length of {value} ({len(value)}) != {self.length}")
        return finished, value


class TypeTextRegex(TypeText):
    def __init__(self, regexp: str, is_nullable=False, name: str = None):
        super().__init__(is_nullable=is_nullable, name=name)
        self.regexp = re.compile(regexp)

    def validate(self, value, parse_from_string: bool = False):
        finished, value = super().validate(value, parse_from_string=parse_from_string)
        if not finished:
            if not self.regexp.match(value):
                raise SchemaError(f"value {value} does not match {self.regexp}")
        return finished, value


class TypeBool(Type):
    def validate(self, value, parse_from_string: bool = False):
        finished, value = super().validate(value, parse_from_string=parse_from_string)
        if not finished:
            if value not in {True, False, 0, 1}:
                raise SchemaError(f"value {value} is not of type bool")
        return finished, value

    def from_string(self, value):
        return {"true": True, "false": False, "0": False, "1": True}[value.lower()]


class TypeNumeric(Type):
    def validate(self, value, parse_from_string: bool = False):
        finished, value = super().validate(value, parse_from_string=parse_from_string)
        if not finished:
            validate_not_python_type(value, bool)
            validate_python_type(value, (int, float, Decimal))
        return finished, value

    def from_string(self, value):
        return float(value)


class TypeInteger(TypeNumeric):
    def validate(self, value, parse_from_string: bool = False):
        finished, value = super().validate(value, parse_from_string=parse_from_string)
        if not finished:
            if (
                isinstance(value, (float, Decimal))
                and not is_null(value)
                and int(value) != value
            ):
                raise SchemaError(f"value {value} is not of type int")
        return finished, value

    def from_string(self, value):
        return int(value)


class TypeDecimal(TypeNumeric):
    def validate(self, value, parse_from_string: bool = False):
        finished, value = super().validate(value, parse_from_string=parse_from_string)
        if not finished:
            validate_python_type(value, Decimal, int)
        return finished, value

    def from_string(self, value):
        return Decimal(value)


class TypeFloat(TypeNumeric):
    def validate(self, value, parse_from_string: bool = False):
        finished, value = super().validate(value, parse_from_string=parse_from_string)
        return finished, value
        # int and decimal are ok as well


class TypeDate(Type):
    def validate(self, value, parse_from_string: bool = False):
        finished, value = super().validate(value, parse_from_string=parse_from_string)
        if not finished:
            validate_python_type(value, datetime.date)
            validate_not_python_type(value, datetime.datetime)
        return finished, value

    def from_string(self, value):
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()


class TypeTime(Type):
    def validate(self, value, parse_from_string: bool = False):
        finished, value = super().validate(value, parse_from_string=parse_from_string)
        if not finished:
            validate_python_type(value, datetime.time)
        return finished, value

    def from_string(self, value):
        return datetime.datetime.strptime(value, "%H-%M-%S").time()


class TypeDatetime(Type):
    def validate(self, value, parse_from_string: bool = False):
        finished, value = super().validate(value, parse_from_string=parse_from_string)
        if not finished:
            validate_python_type(value, datetime.datetime)
        return finished, value

    def from_string(self, value):
        # sometimes, we have also 7 digits
        if len(value) == 27:
            value = value[:26]

        for pattern in [
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ]:
            try:
                return datetime.datetime.strptime(value, pattern)
            except ValueError:
                pass
        raise ValueError(value)


class TypeGeometry(Type):
    def validate(self, value, parse_from_string: bool = False):
        raise NotImplementedError


class Column:
    def __init__(self, name: str, dtype: Type):
        self.name = name
        self.dtype = dtype

    def validate(self, values: Iterable, parse_from_string: bool = False):
        """delegate validation to dtype"""
        try:
            for value in values:
                self.dtype.validate(value, parse_from_string=parse_from_string)
        except Exception as err:
            raise err.__class__(f"{self.name}: {err}")

    @property
    def is_nullable(self):
        return self.dtype.is_nullable

    def __str__(self):
        return self.name


class ColumnCollection:
    """orderd list of columns with unique names, accessible by name"""

    def __init__(self, columns: List[Column], name: str = None):
        self.name = name
        self.columns = columns

        # Dict[str, int]: init index, check unique names
        self.column_index = {}
        for i, c in enumerate(self.columns):
            if c.name in self.column_index:
                raise KeyError(f"Duplicate column name: {c.name}")
            self.column_index[c.name] = i

    def __getitem__(self, key) -> Column:
        if isinstance(key, str):
            key = self.column_index[key]
        column = self.columns[key]
        return column


class Constraint(ColumnCollection):
    def __init__(
        self,
        columns: List[Union[Column, str]],
        name: str = None,
    ):
        # if columns are just names: create dummy columns
        columns = [
            c if isinstance(c, Column) else Column(name=c, dtype=Type())
            for c in columns
        ]

        super().__init__(columns=columns, name=name)
        for column in self.columns:
            if column.is_nullable:
                raise SchemaError(
                    "Constraint column should not be nullable: {column.name}"
                )

    def validate(self, data: DataFrame):
        raise NotImplementedError()

    def __str__(self):
        return "(" + ",".join(c.name for c in self.columns) + ")"


class ConstraintUnique(Constraint):
    def validate(self, value_tuples: Iterable[tuple]):
        """delegate validation to dtype"""
        duplicates = find_duplicates(value_tuples)
        if duplicates:
            raise SchemaError(f"Duplicates in unique columns {self}: {len(duplicates)}")


class ConstraintForeignKey(Constraint):
    def validate(self, value_tuples: Iterable[tuple]):
        logging.warning("ConstraintForeignKey has no validator yet")


class TableSchema(ColumnCollection):
    def __init__(
        self,
        columns: List[Column],
        constraints: List[Constraint] = None,
        name: str = None,
        ignore_empty_columns: bool = False,
        ignore_columns_order: bool = False,
        parse_from_string: bool = False,
    ):
        super().__init__(columns=columns, name=name)
        self.constraints = constraints or []
        self.ignore_empty_columns = ignore_empty_columns
        self.ignore_columns_order = ignore_columns_order
        self.parse_from_string = parse_from_string

    def _validate_column_names(self, data: DataFrame):
        column_names_schema = [c.name for c in self.columns]
        column_names_data = [str(c) for c in data.columns]
        column_names_constraints = set()
        for constraint in self.constraints:
            column_names_constraints = column_names_constraints | set(
                c.name for c in constraint.columns
            )

        # check uniqueness
        duplicates = find_duplicates(column_names_data)
        if duplicates:
            raise SchemaError(f"Duplicate column names: {duplicates}")

        column_names_new = [
            c for c in column_names_data if c not in column_names_schema
        ]
        if column_names_new:
            raise SchemaError(f"data columns not defined in schema: {column_names_new}")
        column_names_schema_in_data = [
            c for c in column_names_schema if c in column_names_data
        ]
        assert len(column_names_schema_in_data) == len(column_names_data)
        assert set(column_names_schema_in_data) == set(column_names_data)

        if not self.ignore_columns_order:
            if tuple(column_names_schema_in_data) != tuple(column_names_data):
                raise SchemaError("schema and data columns not in same order")

        column_names_missing = [
            c for c in column_names_schema if c not in column_names_data
        ]
        if column_names_missing:
            if not self.ignore_empty_columns:
                raise SchemaError(
                    f"schema columns missing in data: {column_names_missing}"
                )
            else:
                column_names_missing_not_nullable = [
                    c
                    for c in column_names_missing
                    if (not self[c].is_nullable or c in column_names_constraints)
                ]
                if column_names_missing_not_nullable:
                    raise SchemaError(
                        "required schema columns missing in data: "
                        f"{column_names_missing_not_nullable}"
                    )

    def validate(self, data: DataFrame):
        try:
            self._validate_column_names(data=data)

            for column_name in data.columns:
                column = self[column_name]
                col_data = data[column_name]
                column.validate(col_data, parse_from_string=self.parse_from_string)

            for constraint in self.constraints:
                cols_data = [data[c.name] for c in constraint.columns]
                cols_data = zip(*cols_data)
                constraint.validate(cols_data)
        except Exception as err:
            raise err.__class__(f"{self.name}: {err}")


class SchemaDialect:
    def to_table_schema(
        self,
        source,
        ignore_empty_columns: bool = False,
        ignore_columns_order: bool = False,
        parse_from_string: bool = False,
    ) -> TableSchema:
        raise NotImplementedError()

    def from_table_schema(self, schema: TableSchema):
        raise NotImplementedError()


class SchemaDialectJsonschema(SchemaDialect):
    def to_table_schema(
        self,
        source: dict,
        ignore_empty_columns: bool = False,
        ignore_columns_order: bool = False,
        parse_from_string: bool = False,
    ) -> TableSchema:
        raise NotImplementedError()

    def from_table_schema(self, schema: TableSchema) -> dict:
        raise NotImplementedError()


class SchemaDialectFrictionless(SchemaDialect):
    def to_table_schema(
        self,
        source: fl.Schema,
        ignore_empty_columns: bool = False,
        ignore_columns_order: bool = False,
        parse_from_string: bool = False,
    ) -> TableSchema:
        raise NotImplementedError()

    def from_table_schema(self, schema: TableSchema) -> fl.Schema:
        raise NotImplementedError()


class SchemaDialectSqlalchemy(SchemaDialect):
    def from_table_schema(self, schema: TableSchema) -> sa.Table:
        raise NotImplementedError()

    def _get_table_object(
        self, connection_string: str, table_name: str, schema_name: str = None
    ) -> sa.Table:
        """Get current  table schema from the database"""
        metadata = sa.MetaData()
        eng = sa_create_engine(connection_string)
        metadata.reflect(bind=eng, only=[table_name], schema=schema_name)
        table = metadata.tables[table_name]
        return table

    def to_table_schema(
        self,
        source: Union[sa.Table, str],
        ignore_empty_columns: bool = False,
        ignore_columns_order: bool = False,
        parse_from_string: bool = False,
    ) -> TableSchema:
        name = source.name

        columns = []
        for column in source.columns:
            columns.append(self._get_column(column))

        constraints = []
        for constraint in source.constraints:
            if isinstance(constraint, sa.PrimaryKeyConstraint):
                const_cols = [self._get_column(c) for c in constraint.columns]
                constraints.append(ConstraintUnique(columns=const_cols))
            elif isinstance(constraint, sa.ForeignKeyConstraint):
                const_cols = [self._get_column(c) for c in constraint.columns]
                constraints.append(ConstraintForeignKey(columns=const_cols))
            else:
                logging.error(f"Not implemented: {type(constraint)}")

        schema = TableSchema(
            columns=columns,
            constraints=constraints,
            name=name,
            ignore_empty_columns=ignore_empty_columns,
            ignore_columns_order=ignore_columns_order,
            parse_from_string=parse_from_string,
        )
        return schema

    def _get_column(self, column: sa.Column) -> Column:
        try:
            dtype = self._get_column_dtype(column.type, column.nullable)
        except NotImplementedError as err:
            raise NotImplementedError(f"{column.name}: {err}")

        return Column(name=column.name, dtype=dtype)

    def _get_column_dtype(self, dtype, is_nullable: bool) -> Type:
        is_nullable = bool(is_nullable)
        dtype_str = str(dtype).upper()
        for pat, func in [
            (r"^DATETIME2$", lambda: TypeDatetime(is_nullable=is_nullable)),
            (r"^DATE$", lambda: TypeDate(is_nullable=is_nullable)),
            (r"^BIT$", lambda: TypeBool(is_nullable=is_nullable)),
            (r"^.*INT.*$", lambda: TypeInteger(is_nullable=is_nullable)),
            (r"^FLOAT$", lambda: TypeFloat(is_nullable=is_nullable)),
            (
                r"^N?VARCHAR\(([0-9]+)\).*$",  # may have COLLATE
                lambda max_length: TypeTextMaxLen(
                    max_length=int(max_length), is_nullable=is_nullable
                ),
            ),
            (
                r"^N?VARCHAR.*$",  # may have COLLATE,MUST COME AFTER "^VARCHAR\(([0-9]+)\).*$"  # noqa
                lambda: TypeText(is_nullable=is_nullable),
            ),
            (
                r"^CHAR\(([0-9]+)\).*$",  # may have COLLATE
                lambda length: TypeTextFixLen(
                    length=int(length), is_nullable=is_nullable
                ),
            ),
        ]:
            match = re.match(pat, dtype_str)
            if match:
                return func(*match.groups())
        else:
            raise NotImplementedError(dtype_str)
