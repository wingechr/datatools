import json

from datatools.utils import json_serialize


def objects_euqal(left, right):
    left = json.dumps(left, sort_keys=True, default=json_serialize)
    right = json.dumps(right, sort_keys=True, default=json_serialize)
    return left == right
