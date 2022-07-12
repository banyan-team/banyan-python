from types import NoneType
from future import Future
from typing import Union
from copy import copy

from plum import dispatch

from utils_partitions import PartitioningConstraintOverGroup

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
        "MATCH_ON=" * string(on),
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
)
    new_relative_to: list = Future[arg]
    new_relative_to.extend(copy(relative_to)
    PartitioningConstraintOverGroup(
        to = "SCALE_BY=$by" if to < 0.0 else "SCALE_TO=$to",
        pt_refs_to_jl(new_relative_to)
    )
)

@dispatch
def Scale (
    arg: Future, 
    to: Union[float, str, NoneType]=None,
    by: float=1.0, 
    relative_to: list = Future
):
    Scale(arg, -1.0 if (to is None) else parse_bytes(to), convert(float, by):float, relative_to)
