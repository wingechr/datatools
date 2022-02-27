import networkx as nx

from .package import Package


class TaskGeneratorBase:
    def generate(self, **role_instances):
        for task in self._generate(**role_instances):
            assert isinstance(task, TaskInput)
            yield task

    def _generate(self, **role_instances):
        raise NotImplementedError


class TaskHandlerBase(Package):
    def __init__(self, name):
        super().__init__(name, resources=[], profile="task-handler")

    def handle(self, task_input):
        # task_input_id = task_input.get_id()
        # task_handler_id = self.get_id()

        # TODO add to log
        # TODO allow for caching here

        task_output = self._handle(task_input)
        # task_output_id = task_output.get_id()

        return task_output

    def _handle(self, task_input):
        raise NotImplementedError


class TaskInput(Package):
    """
    A task input is a package with a name identifying the class of task
    and some names resources, similar to keyword arguments passed to a function
    """

    def __init__(self, task_name, *resources):
        super().__init__(name=task_name, resources=resources, profile="task-input")


class TaskOutput(Package):
    def __init__(self, task_name, *resources):
        super().__init__(name=task_name, resources=resources, profile="task-output")


class TaskOutputError(TaskOutput):
    def __init__(self, task_name, *resources):
        super().__init__(
            name=task_name, resources=resources, profile="task-output-error"
        )


class NodeInstance:
    def __init__(self, task, result):
        assert isinstance(result, TaskOutput)  # TaskOutput or Error
        assert isinstance(task, TaskInput)
        self.task = task
        self.result = result


class TaskGraph:
    def __init__(self):
        self.graph = nx.DiGraph()

    def add_node(self, task_generator, task_handler, **parent_node_roles):
        """
        Returns: node
        """
        node_id = len(self.graph) + 1
        self.graph.add_node(
            node_id,
            task_generator=task_generator,
            task_handler=task_handler,
            instances=None,
        )
        for role_name, parent_node in parent_node_roles or {}:
            self.graph.add_edge(parent_node["node_id"], node_id, role=role_name)

        for cycle in nx.simple_cycles(self.graph):  # generator
            raise Exception("Graph has cycles: %s" % cycle)

        return self.graph.nodes[node_id]

    def execute(self):
        # iterate over nodes in correct order
        # see https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.dag.topological_sort.html # noqa: E501
        for nid in nx.topological_sort(self.graph):
            node = self.graph.nodes[nid]
            task_generator = node["task_generator"]
            task_handler = node["task_handler"]
            assert not node["instances"]
            node["instances"] = []
            role_instances = {}
            for pnid, nid, role in self.graph.in_edges(nid, data="role"):
                instances = self.graph[pnid]["instances"]
                assert instances is not None
                role_instances[role] = instances
            for task in task_generator.generate(**role_instances):
                result = task_handler.handle(task)
                instance = NodeInstance(task=task, result=result)
                node["instances"].append(instance)
