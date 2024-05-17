import json
import logging

from datatools.utils import json_serialize

logging.basicConfig(
    format="[%(asctime)s %(levelname)7s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


def objects_euqal(left, right):
    left = json.dumps(left, sort_keys=True, default=json_serialize)
    right = json.dumps(right, sort_keys=True, default=json_serialize)
    return left == right
