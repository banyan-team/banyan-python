# class Sample:
#     def __init__(
#         self,
#         value:Any,
#         properties:Dict[str,Any]=Dict(),
#         sample_rate:int=None,
#         total_memory_usage=None
#     ):
#         self.value = value
#         self.properties = properties

#         if sample_rate is None:
#             sample_rate = get_job().sample_rate

#         if total_memory_usage is not None:
#             self.properties["memory_usage"] = round(total_memory_usage / sample_rate)
#         self.properties["rate"] = sample_rate