from unittest import TestCase

from datatools.validate import ValExcessFields, ValSql, ValUnique, validate


def n_errors(errors):
    return len(list(errors.values())[0])


class testValidate(TestCase):
    def test_excess(self):
        data = [{"a": 1, "b": "b1"}, {"a": 2, "b": "b1"}, {"a": 3}]
        errors = validate(data, {"excess": ValExcessFields(["a"])})
        self.assertEqual(n_errors(errors), 2)

    def test_unique(self):
        data = [{"a": 1, "b": "b1"}, {"a": 2, "b": "b1"}, {"a": 3, "b": "b2"}]
        errors = validate(data, {"unique": ValUnique(["a"])})
        self.assertEqual(len(errors), 0)
        errors = validate(data, {"unique": ValUnique(["b"])})
        self.assertEqual(n_errors(errors), 1)

    def test_sql(self):
        data = [
            {"a": -1},
            {"a": 2},
            {"a": 3},
            {},
        ]
        errors = validate(data, {"INT": ValSql("a", "INT", val_null=[None])})
        self.assertEqual(len(errors), 0)

        errors = validate(data, {"TINYINT": ValSql("a", "TINYINT", val_null=[None])})
        self.assertEqual(n_errors(errors), 1)

        errors = validate(data, {"TINYINT": ValSql("a", "TINYINT", val_null=None)})
        self.assertEqual(len(errors), 2)  # one not null error, one type error

        data = [
            {"t": "1900-01-02"},
        ]
        errors = validate(data, {"DATETIME": ValSql("t", "DATETIME")})
        self.assertEqual(len(errors), 0)
        self.assertEqual(data[0]["t"], "1900-01-02T00:00:00.000000")
