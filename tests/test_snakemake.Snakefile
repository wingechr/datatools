import os
import sys
from datatools import FileDataStorage, AnnotatedFunction
import json

# example functions
@AnnotatedFunction.wrap(function_id="urn:function:generate")
def generate(n:int=1) -> str:
    """generate a list of 1s"""
    return json.dumps([1] * n)

@AnnotatedFunction.wrap(function_id="urn:function:convert")
def convert(data: list, y:int=1) -> list:
    """add y to each list element"""
    return [x + x for x in data]

# use FileDataStorage to create jobs

data_storage = FileDataStorage(".")
task_generate = data_storage.task(generate, {"output": None}) # "output": None -> already bytes
task_convert = data_storage.task(convert, {"output": json.dump}, {"data": json.load})

# create rules to links jobs

rule convert:
    input:
        "generated.json"
    output:
        "converted.json"
    run:
        task_convert(output[0], input[0])

rule generate:
    output:
        "generated.json"
    run:
        task_generate(output[0], n=10)

# for this simple exmaple, we could also manally just run
# data_storage.task(generate, {"output": None}, skip_finished=True)("generated.json")
# data_storage.task(convert, {"output": dump}, {"data": json.loads}, skip_finished=True)("generated.json", "converted.json")
