from typing import Any, Dict, List, Optional, Union

from .sessions import get_session_id
from .utils import send_request_get_response
from .utils_communication import receive_to_client, send_to_client
from .utils_future_computation import (
    FutureComputation,
    FutureId,
    PartitionType,
    TaskGraph,
    _new_future_id,
    is_future_id,
)


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
        record_task(self, send_to_client, self, {self: "Consolidated"})
        send_request_get_response(
            "run_computation",
            {
                "session_id": get_session_id(),
                "task_graph": self._task_graph.to_dict(),
                "future_ids": [self.id],
            },
        )
        return receive_to_client()


def _futures_to_future_ids(
    futures: Union[Future, List[Future]]
) -> Optional[List[FutureId]]:
    if isinstance(futures, Future):
        return [futures.id]
    elif futures is None:
        return None
    else:
        return [
            (future.id if isinstance(future, Future) else future)
            for future in futures
        ]


def _to_list(l) -> Optional[List]:
    if isinstance(l, List):
        return l
    elif l is None:
        return None
    else:
        return [l]


PartitioningSpec = Union[
    str,
    PartitionType,
    Dict[Union[Future, FutureId], Union[PartitionType, List[PartitionType]]],
]


def record_task(
    results: Union[Future, List[Future]],
    func: Any,
    args: Union[Future, List[Future]],
    partitioning: Union[PartitioningSpec, List[PartitioningSpec]],
    static=None,
):
    args = _to_list(args)
    results = _to_list(results)
    arg_ids = list(filter(is_future_id, _futures_to_future_ids(args)))
    result_ids = _futures_to_future_ids(results)

    # Generate new futures if results or partitioning keys are string variable names
    new_futures: Dict[str, Future] = {}
    for i in range(len(results)):
        if isinstance(results[i], str):
            new_future = Future()
            new_futures[results[i]] = new_future
            results[i] = new_future
    partitioning = _to_list(partitioning)
    for partitioning_map in partitioning:
        if isinstance(partitioning_map, dict):
            for k, v in partitioning_map.items():
                if k in new_futures:
                    partitioning_map[new_futures[k]] = v

    # Construct a `FutureComputation` for the new task`
    fc = FutureComputation(
        func,
        _futures_to_future_ids(args),
        result_ids,
        [
            {
                (f.id if isinstance(f, Future) else f): (
                    pt if isinstance(pt, PartitionType) else PartitionType(pt)
                )
                for f, pt in p.items()
            }
            if isinstance(p, dict)
            else {
                f: p if isinstance(p, PartitionType) else PartitionType(p)
                for f in arg_ids + result_ids
            }
            for p in partitioning
        ],
        static=_futures_to_future_ids(static),
        name=func.__name__,
    )

    # Record the task for each result
    for result in results:
        # Record each task required to construct the arguments
        for arg in args:
            if isinstance(arg, Future):
                for task in arg._task_graph:
                    result._record_task(task)

        # Record the new task
        result._record_task(fc)

    return results
