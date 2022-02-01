from typing import Dict, List


JobId = str
ValueId = str

class Job:
    def __init__(
        self,
        id:JobId,
        cluster_name:str,
        nworkers:int,
        sample_rate:int,
        locations:Dict[ValueId,Location]=Dict(),
        pending_requests:List[Request] = [],
        futures_on_client = Dict(), # TODO: WeakKeyDict:Dict[ValueId,Future]???
    ):
        self.id = id
        self.nworkers = nworkers
        self.sample_rate = sample_rate
        self.locations = locations
        self.pending_requests = pending_requests
        self.futures_on_client = futures_on_client
        self.cluster_name = cluster_name
