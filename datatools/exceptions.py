class DatatoolsException(Exception):
    pass


class NonzeroReturncode(DatatoolsException):
    pass


class DataExists(DatatoolsException):
    pass


class InvalidPath(DatatoolsException):
    pass


class DataDoesNotExists(DatatoolsException):
    pass
