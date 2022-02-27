from datatools.package import DataResource
from datatools.task import (
    TaskGeneratorBase,
    TaskGraph,
    TaskHandlerBase,
    TaskInput,
    TaskOutput,
)

from . import TestCase


class GenerateNumberToFactorize(TaskGeneratorBase):
    def _generate(self, **role_instances):
        assert not role_instances  # this generator has no input
        yield TaskInput("factorize", DataResource("number", 20))


class Factorize(TaskHandlerBase):
    def _handle(self, task_input):
        assert len(task_input.resources) == 1
        number = task_input.get_resource("number").data
        resources = [
            DataResource("factor_%d" % i, f)
            for i, f in enumerate(self.factorize(number))
        ]
        return TaskOutput("Factors", *resources)

    def factorize(self, x):
        f = 2
        while x > 1 and f <= x:
            if x / f == x // f:
                x = x // f
                yield f
            else:
                f += 1


class TestTaskGraph(TestCase):
    def test_graph(self):
        tg = TaskGraph()
        node1 = tg.add_node(
            task_generator=GenerateNumberToFactorize(),
            task_handler=Factorize("factorize"),
        )
        tg.execute()
        factors = [x.data for x in node1["instances"][0].result.resources]
        self.assertEqual(factors, [2, 2, 5])
