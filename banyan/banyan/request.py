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


class RecordLocationRequest(Request):
    def __init__(self, value_id: ValueId, location: Location):
        self.value_id = value_id
        self.location = location


class DestroyRequest(Request):
    def __init__(self, value_id: ValueId):
        self.value_id = value_id
