class Task:
    """
    A simple task class to represent a unit of work in a data pipeline.
    """

    def __init__(self, name, func):
        self.name = name
        self.func = func
        self.downstream = []
        self.result = None

    def __rshift__(self, other):
        self.downstream.append(other)
        return other

    def run(self, executed=None):
        if executed is None:
            executed = set()
        if self.name in executed:
            return self.result

        self.result = self.func()
        executed.add(self.name)

        for t in self.downstream:
            t.run(executed)

        return self.result
