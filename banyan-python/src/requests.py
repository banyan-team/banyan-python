# class RecordTaskRequest:
#     def __init__(self, task:DelayedTask):
#         self.task = task

# class RecordLocationRequest:
#     def __init__(
#         self,
#         value_id:ValueId,
#         location:Location
#     ):
#         self.value_id = value_id
#         self.location = location

# class DestroyRequest:
#     def __init__(
#         self,
#         value_id:ValueId,
#     ):
#         self.value_id = value_id

# Request = Union[RecordTaskRequest, RecordLocationRequest, DestroyRequest]