# TODO: Future class, compute, partitioned_computation

from typing import Any, Dict, List, Optional, Union
from utils_communication import receive_to_client, send_to_client
from sessions import get_session_id
from utils import send_request_get_response
from utils_future_computation import FutureComputation, PartitionType, _new_future_id, FutureId, TaskGraph

class Future:
    def __init__(self):
        self._id: FutureId = _new_future_id()
        self._task_graph: TaskGraph = []

    @property
    def id(self):
        return self._id

    def _record_task(self, fc: FutureComputation):
        self._task_graph.append(fc)

    def compute(self):
        self._record_task(
            FutureComputation(
                send_to_client,
                [self.id],
                [self.id],
                [{self.id: PartitionType("Consolidated")}]
                name="bn_send_to_client",
            )
        )
        resp = send_request_get_response(
            "run_computation",
            {
                "session_id": get_session_id(),
                "task_graph": self._task_graph.to_dict()
                "future_ids": [self.id]
            }
        )
        return receive_to_client()

def _futures_to_future_ids(futures: Union[Future,List[Future]]) -> Optional[List[FutureId]]:
    if isinstance(futures, Future):
        return [futures.id]
    elif futures is None:
        return None
    else:
        return [(future.id if isinstance(future, Future) else future) for future in futures]

def _to_list(l) -> Optional[List]:
    if isinstance(l, List):
        return l
    elif l is None:
        return None
    else:
        return [l]

def record_task(
    results: Union[Future,List[Future]],
    func: Any,
    args: Union[Future,List[Future]],
    partitioning: List[Dict[Future, Union[PartitionType, List[PartitionType]]]],
    static=None
):
    fc = FutureComputation(
        func,
        _futures_to_future_ids(args),
        _futures_to_future_ids(results),
        [
            {
                (f.id if isinstance(f, Future) else f): _to_list(pts)
                for f, pts in p.items()
            }
            for p in partitioning
        ],
        static=_futures_to_future_ids(static),
        name=func.__name__
    )
    for result in _to_list(results):
        result._record_task(fc)
