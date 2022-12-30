# NOTE: This file is copied from banyan-python but could be removed after
# banyan-python is published.

import json
import random
import string
from copy import deepcopy
from hashlib import md5
from typing import Any, Dict, List, Optional, Set, Union

from typing_extensions import Self
from utils_serialization import from_str, to_str

"""ID of a future computation created on the client side"""
FutureId = str


"""ID of a partition type for use in the executor"""
PartitionTypeId = str


def is_future_id(id: FutureId) -> bool:
    return isinstance(id, FutureId) and id.startswith("bn_fut_")


def _new_future_id() -> FutureId:
    return "bn_fut_" + "".join(
        random.choice(string.ascii_lowercase) for i in range(10)
    )


class PartitionType:
    """
    Specifies how data can be partitioned

    Attributes
    ----------
    name : str
        The primary identifier of the partition type. E.g. - "Blocked",
        "Grouped", or "Consolidated"
    params : Dict[str, Any]
        Additional information about how the data is partitioned.

        For example,
        a Grouped partition type may have `params` set to
        `{"key": "AgeSegment"}` to indicate that the data should be distributed
        across workers such that all the data for an age segment are on the
        same worker.
    id : PartitionTypeId
        This identifies a partition type.

        It's based solely on the name and
        parameters though it could in the future be made such that it is also
        preserved even after merged with other partition types.
    """

    def __init__(
        self,
        name: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Self:
        self._name = name
        self._params = params if params is not None else {}
        self._id = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def params(self) -> Dict[str, Any]:
        return self._params

    @property
    def id(self) -> PartitionTypeId:
        if self._id is None:
            self._id = md5(
                json.dumps(
                    {
                        **self._params,
                        "bn_pt_name": self._name,
                    },
                    sort_keys=True,
                ).encode()
            ).hexdigest()
        return self._id

    def __eq__(self, __o: object) -> bool:
        return self.id == __o.id

    def __hash__(self) -> int:
        return int(self.id, 16)

    def _merge_with(self, other: Self, only_check=False) -> bool:
        """
        Merges another `PartitionType` into self's parameters

        Merging requires name and parameters to match. Parameters match when
        there isn't a different value for the same key. A static PT _can_ be
        merged with a non-static PT. For a given `List[FutureComputation]`,
        after PTs are merged, they are reassigned to each future and if the
        PT was previously static for that future, the new merged PT is set to
        be static for it. For example, two PTs with parameters
        `{"name": "Grouped", "reverse": True}` and
        `{"name": "Grouped", "key": "species"}` can
        be merged to produce
        `{"name": "Grouped", "key": "species", "reverse": True}`.
        """

        # Quick checks
        if self.name != other.name:
            return False
        if len(other.params) == 0 or self.params == other.params:
            return True
        if len(self.params) == 0:
            self._params = other.params
            return True

        # Check for inconsistency between the 2 PTs
        if self.params != other.params:
            for ak, av in self.params.items():
                if ak in other.params and av != other.params[ak]:
                    return False

        # Update a with parameters in b and return
        if not only_check:
            self.params.update(other.params)

        return True

    @property
    def is_blocked(self) -> bool:
        """
        Standard partition type for data that is equally  split across workers

        It is not guaranteed that data with blocked partitioning is also
        balanced, which is a key precondition for operations like horizontal
        concatenation of tables. Also, it is not guaranteed that the order
        of data is preserved which may be key for some computation. For these
        scenarios, new partition types may be developed.
        """
        return self.name == "Blocked"

    @property
    def is_grouped(self) -> bool:
        """
        Standard partition type for data that is grouped by some "key" and
        distributed across workers such that each worker has distinct groups
        """
        return self.name == "Grouped"

    @property
    def is_consolidated(self) -> bool:
        """
        Standard partition type for data that is consolidated on the main
        worker
        """
        return self.name == "Consolidated"

    @property
    def is_none(self) -> bool:
        """
        Standard partition type for data that requires some final computation
        before being returned to the user

        For example, the result of some aggregation (a `PartialAggregation`)
        may be assigned the None partition type so that
        `convert_partition_type` (converting from None to Consolidated)
        completes the aggregation.
        """
        return self.name == "None"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self._name,
            "params": self._params,
        }

    def from_dict(data: Dict[str, Any]) -> Self:
        return PartitionType(data["name"], data["params"])

    def __str__(self) -> str:
        params = {k: v for k, v in self.params.items() if k != "name"}
        params_str = ", ".join([f"{k}: {v}" for k, v in params.items()])
        return f"{self.name}({params_str})"


def _arg_to_dict_or_str(arg: Any) -> Union[str, Dict[str, Any]]:
    if is_future_id(arg):
        return arg
    elif isinstance(arg, PartitionType):
        return arg.to_dict()
    else:
        return to_str(arg)


def _arg_from_dict_or_str(
    arg: Union[str, Dict[str, Any]], use_cloudpickle=False
) -> Any:
    if is_future_id(arg):
        return arg
    elif isinstance(arg, dict):
        return PartitionType.from_dict(arg)
    elif use_cloudpickle:
        return from_str(arg)
    else:
        return arg


Partitioning = Dict[FutureId, PartitionType]
PartitioningMulti = Dict[FutureId, List[PartitionType]]


def make_partitioning(
    future_ids: List[FutureId], pt: PartitionType
) -> Partitioning:
    return {future_id: deepcopy(pt) for future_id in future_ids}


def _partitioning_from_dict(d: Dict[str, Any]) -> PartitioningMulti:
    return {
        fid: [PartitionType.from_dict(pt) for pt in pts]
        for fid, pts in d.items()
    }


def _partitioning_to_dict(partitioning: PartitioningMulti) -> Dict[str, Any]:
    return {
        fid: [pt.to_dict() for pt in pts] for fid, pts in partitioning.items()
    }


class FutureComputation:
    """
    Info to perform some computation in the future
    in parallel across multiple workers

    Attributes
    ----------
    func : Any
        The function to run for the computation. This may be pickled.

        This is either a function or a string for a special function like
        "convert_partition_type".
    args : List[Union[FutureId, Any]]
        The arguments for the computation. They could either be IDs of futures
        or constant values (that may be pickled).
    results : List[FutureId]
        The IDs of futures that result from this computation
    partitioning_list : List[Dict[FutureId, PartitionType]]
        The allowed partitioning for this computation. When this computation is
        scheduled, all of the arguments and results must be partitioned
        according to one of the elements of this list.
    static : List[FutureId]
        List of futures that have static partition types, meaning they cannot
        be converted to some other partitioning.

        For example, the future for a
        `pl.GroupBy` might be assigned a static blocked partitioning because
        once assigned it cannot be directly converted to another partitioning.
        Instead, the input to the `pl.groupby` would have to be converted and
        then the future could be recomputed. This field is only meaningful for
        PTs assigned to future results in the given task graph.
    name : str
        Optional name of the computation

    Examples
    --------
    >>> FutureComputation(
        pl.GroupBy.agg,
        [, "AgeSegment"],
    )
    """

    def __init__(
        self,
        func: Any,
        args: List[Union[FutureId, Any]],
        results: List[FutureId],
        partitioning: List[Partitioning],
        static: Union[List[FutureId], None] = None,
        name: Union[str, None] = None,
    ) -> Self:
        self.func = func
        self.args = args
        self.results = results
        self.partitioning_list = partitioning
        self.name = name or "unknown_func"
        self.static = static or []
        self._assert_valid()

    def _assert_valid(self):
        for partitioning in self.partitioning_list:
            partitioning: Partitioning
            curr_future_ids = list(partitioning.keys())
            if set(curr_future_ids) != set(self.future_ids):
                raise ValueError(
                    f"Annotation of future computation {self.name} has "
                    f"future IDs {', '.join(self.future_ids)} but a specified "
                    "partitioning only assigns partition types to "
                    f"{', '.join(curr_future_ids)}."
                )

    @property
    def future_ids(self) -> List[FutureId]:
        return [e for e in self.args + self.results if is_future_id(e)]

    @property
    def is_specialized(self) -> bool:
        """
        Returns whether the partitioning is specialized such that there is a
        single allowable partition type for each future in this computation
        """
        return len(self.partitioning_list) == 1

    @property
    def specialized_partitioning(self):
        assert self.is_specialized
        return self.partitioning_list[0]

    def is_static(self, fut: FutureId) -> bool:
        return fut in self.static

    def to_dict(self) -> Dict[str, Any]:
        return {
            "func": self.func
            if isinstance(self.func, str)
            else to_str(self.func),
            "results": self.results,
            "args": [_arg_to_dict_or_str(arg) for arg in self.args],
            "partitioning": [
                {
                    future_id: pt.to_dict()
                    for future_id, pt in pt_assignment.items()
                }
                for pt_assignment in self.partitioning_list
            ],
            "static": self.static,
            "name": self.name,
        }

    def from_dict(data: Dict[str, Any], use_cloudpickle=True) -> Self:
        return FutureComputation(
            from_str(data["func"]) if use_cloudpickle else data["func"],
            [
                _arg_from_dict_or_str(arg, use_cloudpickle)
                for arg in data["args"]
            ],
            data["results"],
            [
                {
                    future_id: PartitionType.from_dict(pt)
                    for future_id, pt in pt_assignment.items()
                }
                for pt_assignment in data["partitioning"]
            ],
            static=data.get("static", []),
            name=data.get("name", None),
        )

    def __str__(self) -> str:
        arg_abbrevs = [
            arg if is_future_id(arg) else str(arg)[0:16] for arg in self.args
        ]
        args_with_static = [
            ("static " + arg_abbrev)
            if is_future_id(arg_abbrev) and arg_abbrev in self.static
            else arg_abbrev
            for arg_abbrev in arg_abbrevs
        ]
        fc_signature = (
            f"{self.name}("
            f"{', '.join(args_with_static)}) -> {', '.join(self.results)}"
        )
        return fc_signature


TaskGraph = List[FutureComputation]


def _get_future_ids_to_abbrevs(
    future_ids: Set[FutureId],
) -> Dict[FutureId, str]:
    if len(future_ids) == 0:
        return {}
    fid_len = max([len(fid) for fid in future_ids])
    for abbrev_len in range(len("bn_fut_") + 1, fid_len + 1):
        future_id_abbrevs = set([fid[0:abbrev_len] for fid in future_ids])
        if len(future_id_abbrevs) == len(future_ids):
            fid_len = abbrev_len
            break
    future_ids_to_abbrevs = {fid: fid[0:fid_len] for fid in future_ids}
    return future_ids_to_abbrevs


def print_task_graph(tg: TaskGraph, display=True):
    if len(tg) == 0:
        return tg

    # Map each future ID to an abbreviation
    future_ids = set()
    for fc in tg:
        for fid in fc.future_ids:
            future_ids.add(fid)
    future_ids_to_abbrevs = _get_future_ids_to_abbrevs(future_ids)

    # Print out info
    res = ""
    for i, fc in enumerate(tg):
        fc: FutureComputation
        res += f"Task #{i + 1}: " + str(fc) + "\n"

        for j, partitioning in enumerate(fc.partitioning_list):
            partitioning: Partitioning
            pts_str = ", ".join(
                [f"{fid}: {str(pt)}" for fid, pt in partitioning.items()]
            )
            res += f"\tPartitioning #{j + 1}: " + pts_str + "\n"
    for fid, abbrev in future_ids_to_abbrevs.items():
        res = res.replace(fid, abbrev)
    res = res.replace("bn_fut_", "")
    res = res[0:-2]  # Remove last '\n'

    if display:
        print(res)
    else:
        return res


def print_partitioning(partitioning: Union[Partitioning, PartitioningMulti]):
    print("Partitioning:")
    future_ids = set(partitioning.keys())
    future_ids_to_abbrevs = _get_future_ids_to_abbrevs(future_ids)
    for fid, pts in partitioning.items():
        fid_abbrev = future_ids_to_abbrevs[fid]
        print(f"\t{fid_abbrev}: {', '.join(map(str, pts))}")
    if len(future_ids) == 0:
        print("\tempty")
