import json
import logging


def objects_euqal(left, right):
    left = json.dumps(left, sort_keys=True)
    right = json.dumps(right, sort_keys=True)
    return left == right


logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s", level=logging.DEBUG
)
