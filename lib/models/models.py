class Task:
    def __init__(self, id=None, execution_time=None, type=None, data=None, repeat=None, repeat_type=None):
        self.id = id
        self.execution_time = execution_time
        self.type = type
        self.data = data
        self.repeat = repeat
        self.repeat_type = repeat_type


class UpdateTask:
    def __init__(self, id, execution_time=None):
        self.id = id
        self.execution_time = execution_time
