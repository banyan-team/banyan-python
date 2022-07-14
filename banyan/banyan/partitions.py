from types import NoneType
from future import Future
from typing import Union, List
from copy import copy

from plum import dispatch

from utils_partitions import (
    PartitioningConstraintOverGroup, 
    PartitionType, 
    PartitioningConstraints, 
    PartitioningConstraint,
    PartitionedUsingFunc
)

def pt_ref_to_jl (f: Future):
    return (f.value_id, 0)

def pt_refs_to_jl (refs: list):
    return map(pt_ref_to_jl, refs)

def Cross (args:list):
    return PartitioningConstraintOverGroup("CROSS", pt_refs_to_jl(args))

def Match (args: list):
    return PartitioningConstraintOverGroup("MATCH", pt_refs_to_jl(args))

def MatchOn (on:str, args: list):
    return PartitioningConstraintOverGroup(
        "MATCH_ON=" * str(on),
        pt_refs_to_jl(args),
    )

def AtMost (npartitions: int, f: Future):
    return PartitioningConstraintOverGroup(
        "AT_MOST=$npartitions",
        [pt_ref_to_jl(f)]
    )

@dispatch
def Scale(
    arg: Future, 
    to: float, 
    by: float, 
    relative_to: list
):
    new_relative_to: list = Future[arg]
    new_relative_to.extend(copy(relative_to))
    PartitioningConstraintOverGroup(
        to = "SCALE_BY=$by" if to < 0.0 else "SCALE_TO=$to",
        pt_refs_to_jl(new_relative_to)
    )

@dispatch
def Scale (
    arg: Future, 
    to: Union[float, str, NoneType]=None,
    by: float=1.0, 
    relative_to: list = Future
):
    Scale(arg, -1.0 if (to is None) else parse_bytes(to), convert(float, by):float, relative_to)

PTOrPTUnion = Union[PartitionType, List[PartitionType]]

def merge_pts (a: PartitionType, b: PartitionType, pts_so_far: List[PartitionType]):
    all_params_matching = True

    for param_name in keys(a.parameters):
        if b.parameters.haskey(param_name):
            all_params_matching = all_params_matching and a.parameters[param_name] == b.parameters[param_name]
        if not all_params_matching:
            break

    new_constraints = []

    for c in a.constraints.constraints:
        new_constraints.append(c)
    for c in b.constraints.constraints:
        new_constraints.append(c)

    if all_params_matching:
        pts_so_far.append(PartitionType(merge(a.parameters, b.parameters), PartitioningConstraints(new_constraints)))

def And (a: PTOrPTUnion, b: PTOrPTUnion):
    if isinstance(a, PartitionType) and isinstance(b, PartitionType):
        res = []
        for a_pt in a:
            merge_pts(a_pt, b, res)
        return res
    elif isinstance(a, List[PartitionType]) and isinstance(b, PartitionType):
        res = []
        for a_pt in a:
            merge_pts(a_pt, b, res)
        return res
    elif isinstance(a, List[PartitionType]) and isinstance(b, List[PartitionType]):
        res = []
        for a_pt in a:
            for b_pt in b:
                merge_pts(a_pt, b_pt, res)
        return res
    elif isinstance(a, PartitionType) and isinstance(b, List[PartitionType]):
        return b and a

def Or(a: PTOrPTUnion, b: PartitionType):
    return a + b

def Partitions():
    Partitions(dict{ValueId,PartitionTypeComposition}())

def PartitionAnnotation(): 
    PartitionAnnotation(Partitions(), PartitioningConstraints())

NOTHING_PARTITIONED_USING_FUNC = PartitionedUsingFunc(
    False,
    Future,
    False,
    int,
    tuple{Future,List[int]}[],
    False,
    False,
    True
)


