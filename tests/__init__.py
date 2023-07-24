import json


def objects_euqal(left, right):
    left = json.dumps(left, sort_keys=True)
    right = json.dumps(right, sort_keys=True)
    return left == right
