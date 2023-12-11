class DatatoolsException(Exception):
    """Base class for Exceptions in this package"""

    pass


class NonzeroReturncode(DatatoolsException):
    pass


class DataExists(DatatoolsException):
    pass


class InvalidPath(DatatoolsException):
    pass


class DataDoesNotExists(DatatoolsException):
    pass


class IntegrityError(DatatoolsException):
    pass


class ValidationError(DatatoolsException):
    pass


class SchemaError(DatatoolsException):
    pass
