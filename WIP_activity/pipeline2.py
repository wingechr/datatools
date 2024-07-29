# import datetime
import hashlib
import json
from typing import Callable


class DataRepository:
    """Serialize, store, deserialize data."""

    def load_data(self, uri: str) -> object:
        return uri

    def store_data(self, uri: str, data: object) -> str:
        pass


class MetadataRepository:
    """Query and update RDF triples."""

    def query_output_uri(self, task_uri: str) -> str:
        return None

    def add(self, subject: str, predicate: str, object: str):
        pass


class Process:
    """Run and annotate data pipeline."""

    pass


def get_function_uri(function: Callable):
    # dummy
    return "urn:function:" + function.__name__


def get_argument_uri(argument: str):
    # dummy
    return argument


def get_task_uri(function_uri: str, input_uris: dict) -> str:
    bytes = json.dumps(
        (function_uri, input_uris), indent=0, sort_keys=True, ensure_ascii=False
    ).encode()
    return "urn:md5:" + hashlib.md5(bytes).hexdigest()


def get_job_uri(task_uri) -> str:
    # datetime
    return task_uri + ".datetime"


def generate_output_uri(output: object) -> str:
    pass


def make_decorator(force: bool = False):
    def decorator(function):
        function_uri = get_function_uri(function)

        metadata_repo = MetadataRepository()
        data_repo = DataRepository()

        def decorated_function(**input_arguments):
            input_uris = {
                input_name: get_argument_uri(input_value)
                for input_name, input_value in input_arguments.items()
            }
            task_uri = get_task_uri(function_uri, input_uris)
            # job_uri = get_job_uri(task_uri)

            def get_output():

                # here: decide whether or not to actually run function
                # or to use cached results based on task_uri
                if not force:
                    output_uri = metadata_repo.query_output_uri(task_uri)
                    if output_uri:
                        # try-catch?
                        output = data_repo.load_data(output_uri)
                        return output, output_uri

                # load/parse input data
                input_values = {
                    name: data_repo.load_data(uri) for name, uri in input_uris.items()
                }
                # actually run function
                output = function(**input_values)

                # get output uri
                output_uri = generate_output_uri(output)
                data_repo.store_data(output_uri, output)

                return output, output_uri

            output, output_uri = get_output()

            # metadata
            # link inputs, output, task, job, and context

            return output

        return decorated_function

    return decorator


@make_decorator(force=False)
def example_function(input1: object, input2: object) -> object:
    return input1 + input2


def main():
    input1_uri = "file://input1.json"
    input2_uri = "urn:rdf:int:10"

    result = example_function(input1=input1_uri, input2=input2_uri)
    print(result)


if __name__ == "__main__":
    main()
