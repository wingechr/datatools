"""
Have a look at the specs at
* https://json-schema.org/draft/2019-09/json-schema-validation.html#rfc.section.6.1
* https://specs.frictionlessdata.io/table-schema/#field-descriptors
* https://github.com/frictionlessdata/specs

"""

import unittest
import jsonschema


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
    validator_cls = jsonschema.validators.validator_for(schema)
    # check if schema is valid
    validator_cls.check_schema(schema)
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
            raise ValueError(err_str)

    return validator_function


class TestJsonSchema(unittest.TestCase):
    def test_invalid_schema(self):
        self.assertRaises(
            jsonschema.exceptions.SchemaError,
            get_jsonschema_validator,
            {"type": "invalid"},
        )

    def test_type(self):
        """
        The value of this keyword MUST be either a string or an array.
        If it is an array, elements of the array MUST be strings and MUST be unique.

        Options: "null", "boolean", "object", "array", "number", "string", "integer"
        """

        validator = get_jsonschema_validator({"type": ["string", "boolean"]})
        validator("i am string")
        validator(True)
        self.assertRaises(Exception, validator, 1)

        validator = get_jsonschema_validator({"type": "integer"})
        self.assertRaises(Exception, validator, "100")
        validator(100)

    def test_enum(self):
        """
        The value of this keyword MUST be an array. This array SHOULD have at least one
        element. Elements in the array SHOULD be unique.
        """

        validator = get_jsonschema_validator({"enum": ["v1", "v2"]})
        validator("v1")
        self.assertRaises(Exception, validator, "V2")

    def test_maximum_minimum(self):
        """
        The value of "maximum" MUST be a number, representing an inclusive upper limit
        for a numeric instance.
        The value of "exclusiveMaximum" MUST be number, representing an exclusive upper
        limit for a numeric instance.

        Note: this ONLY validates numbers, other values are ok
        """

        validator = get_jsonschema_validator({"maximum": 100})
        validator(-1)
        validator(100)
        self.assertRaises(Exception, validator, 100.01)

        validator = get_jsonschema_validator({"exclusiveMaximum": 100})
        validator(-1)
        self.assertRaises(Exception, validator, 100)

        validator = get_jsonschema_validator({"exclusiveMinimum": float("-inf")})
        validator(0)
        self.assertRaises(Exception, validator, float("-inf"))

        # non numeric are ok
        validator = get_jsonschema_validator({"maximum": 100})
        validator("1000000")

    def test_min_max_length(self):
        """
        The value of this keyword MUST be a non-negative integer.
        A string instance is valid against this keyword if its length is less than, or
        equal to, the value of this keyword.

        Note: this ONLY validates strings, other values are ok
        """

        validator = get_jsonschema_validator({"minLength": 5, "maxLength": 5})
        validator("09112")
        self.assertRaises(Exception, validator, "9112")

        # non string are ok
        validator(9112)

    def test_min_max_items(self):
        """
        The value of this keyword MUST be a non-negative integer.
        An array instance is valid against "maxItems" if its size is less than, or equal
        to, the value of this keyword.
        """

        validator = get_jsonschema_validator({"minItems": 1, "maxItems": 2})
        validator(None)  # only tests arrays
        validator([1])
        validator([1, None])
        self.assertRaises(Exception, validator, [])
        self.assertRaises(Exception, validator, [1, 2, 3])

    def test_required(self):
        """
        The value of this keyword MUST be an array.
        Elements of this array, if any, MUST be strings, and MUST be unique.

        An object instance is valid against this keyword if every item in the array is
        the name of a property in the instance.
        """

        validator = get_jsonschema_validator({"required": ["a", "b"]})
        validator({"a": 1, "b": 2, "c": 3})
        self.assertRaises(Exception, validator, {"a": 1, "c": 3})

    def test_pattern(self):
        """
        The value of this keyword MUST be a string.
        This string SHOULD be a valid regular expression, according to the
        ECMA 262 regular expression dialect.
        """

        validator = get_jsonschema_validator({"pattern": "^a.*$"})
        validator("aa")
        self.assertRaises(Exception, validator, "Aa")  # case sensitive

        validator = get_jsonschema_validator({"pattern": "a[0-9]{1,2}"})
        validator("a1x")
        validator("a11x")
        self.assertRaises(Exception, validator, "ax")
