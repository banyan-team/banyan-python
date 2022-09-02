from typing import Union

from locations import Location
from partitions import ValueId
from task import DelayedTask


class Request:
    """
    Base class for a Request.
    All new Request classes should subclass from this.
    """

    pass


class RecordTaskRequest(Request):
    def __init__(self, task: DelayedTask):
        self.task = task

    def to_py(self):
        return {"type": "RECORD_TASK", "task": self.task.to_py()}


class RecordLocationRequest(Request):
    def __init__(self, value_id: ValueId, location: Location):
        self.value_id = value_id
        self.location = location

    def to_py(self):
        return {
            "type": "RECORD_LOCATION",
            "value_id": self.value_id,
            "location": self.location.to_py(),
        }


class DestroyRequest(Request):
    def __init__(self, value_id: ValueId):
        self.value_id = value_id

    def to_py(self):
        return {"type": "DESTROY", "value_id": self.value_id}
