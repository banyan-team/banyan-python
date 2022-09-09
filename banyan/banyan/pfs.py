from copy import copy, deepcopy
from functools import reduce
import logging
import pickle
from typing import Any, Callable, Dict, NoneType, Union

try:
    from mpi4py import MPI
except ImportError:
    logging.warning("mpi4py cannot be initialized because MPI is not installed")
from plum import dispatch

from .queues import receive_from_client, send_to_client
from .utils import Empty, empty_handler, indexapply
from .utils_pfs import (
    find_worker_idx_where,
    gather_across,
    getpath,
    get_divisions,
    get_npartitions,
    get_nworkers,
    get_partition_idx,
    get_worker_idx,
    is_main_worker,
    merge_on_executor,
    reduce_and_sync_across,
    split_len,
    sync_across,
)

# This file contains a library of functions for splitting/casting/merging
# partition types (PTs). Any `pfs.py` should have a corresponding
# `pf_dispatch_table.toml` that contains an annotation for each
# splitting/casting/merging that describes how data should be partitioned
# in order for that function to be applicable.

parents = {}


def set_parent(child, parent):
    global parents
    parents[id(child)] = parent


def get_parent(child):
    global parents
    return parents.get(id(child), None)


def forget_parent(child):
    global parents
    parents.pop(child)


def forget_parents():
    global parents
    parents = {}


###################################
# Splitting and merging functions #
###################################


@dispatch
def ReturnNull(
    src,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    return None


@dispatch
def ReturnNull(
    src,
    part,
    params,
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name,
    loc_params,
):
    src = None
    return src


# format_available_memory() =
#     format_bytes(Sys.free_memory()) * " / " * format_bytes(Sys.total_memory())

# sortablestring(val, maxval) = _sortablestring(string(val), string(maxval))
def sortablestring(val, maxval):
    val, maxval = str(val), str(maxval)
    s = val
    maxs = maxval
    res = ["0" for _ in range(len(maxs))]
    res[len(res) - len(s) : len(res)] = list(s)
    return "".join(res)


splitting_divisions = {}


def get_splitting_divisions():
    global splitting_divisions
    return splitting_divisions


def set_in_splitting_divisions(key, val):
    global splitting_divisions
    splitting_divisions[id(key)] = val


def get_from_splitting_divisions(key):
    global splitting_divisions
    return splitting_divisions[id(key)]


def delete_from_splitting_divisions(key):
    global splitting_divisions
    splitting_divisions.pop(id(key))


def id_dict_pop(d, key):
    d.pop(id(key))


def ReadGroupHelper(ReadBlockFunc, ShuffleFunc):
    def ReadGroupHelperFunc(
        src,
        params: Dict[str, Any],
        batch_idx: int,
        nbatches: int,
        comm: MPI.Comm,
        loc_name: str,
        loc_params: Dict[str, Any],
        divisions: list,
        key,
        rev,
    ):
        # TODO: Store filters in parameters of the PT and use them to do
        # partition pruning, avoiding reads that are unnecessary

        # Get information needed to read in the appropriate group
        nworkers = get_nworkers(comm)
        npartitions = nworkers * nbatches
        partition_divisions = get_divisions(divisions, npartitions)
        # TODO: Do some reversing here instead of only doing it later in Shuffle
        # to ensure that sorting in reverse order works correctly

        # The first and last partitions (used if this lacks a lower or upper bound)
        # must have actual division(s) associated with them. If there is no
        # partition that has divisions, then they will all be skipped and -1 will
        # be returned. So these indices are only used if there are nonempty
        # divisions.
        hasdivision = any([(len(d) != 0) for d in partition_divisions])
        firstdivisionidx = next(
            i
            for i in range(len(partition_divisions))
            if (len(partition_divisions[i] != 0))
        )
        lastdivisionidx = next(
            i
            for i in range(len(partition_divisions), -1, -1, -1)
            if (len(partition_divisions[i] != 0))
        )
        firstbatchidx = None
        lastbatchidx = None

        # Get the divisions that are relevant to this batch by iterating
        # through the divisions in a stride and consolidating the list of divisions
        # for each partition. Then, ensure we use boundedlower=true only for the
        # first batch and boundedupper=true for the last batch.
        curr_partition_divisions = []
        for worker_division_idx in range(nworkers):
            for batch_division_idx in range(nbatches):
                # partition_division_idx =
                #     (worker_division_idx - 1) * nbatches + batch_division_idx
                partition_division_idx = get_partition_idx(
                    batch_division_idx, nbatches, worker_division_idx
                )
                if batch_division_idx == batch_idx:
                    # Get the divisions for this partition
                    p_divisions = partition_divisions[partition_division_idx]

                    # We've already used `get_divisions` to get a list of min-max
                    # tuples (we call these tuples "divisions") for each partition
                    # that `ReadGroup` produces. But we only want to read in all
                    # the partitions relevant for this batch. But it is important
                    # then that `curr_partition_divisions` has an element for each
                    # worker. That way, when we call `Shuffle`, it will properly
                    # read data onto each worker that is in the appropriate
                    # partition.
                    curr_partition_divisions = curr_partition_divisions.append(
                        p_divisions
                    )

                # Find the batches that have the first and last divisions
                if partition_division_idx == firstdivisionidx:
                    firstbatchidx = batch_division_idx
                if partition_division_idx == lastdivisionidx:
                    lastbatchidx = batch_division_idx

        # TODO: Call ReadBlockFunc with nbatches=1 and pass in a function as
        # filtering_op in the params
        # TODO: Pass in function calling SplitGroup with

        # Read in each batch and shuffle it to get the data for this partition
        read_block_params = deepcopy(params)
        # We need the divisions by worker for both SplitGroup and Shuffle
        params["divisions_by_partition"] = curr_partition_divisions  # for SplitGroup
        params["consolidate"] = True  # for SplitGroup
        params["boundedlower"] = (not hasdivision) or (batch_idx != firstbatchidx)
        params["boundedupper"] = (not hasdivision) or (batch_idx != lastbatchidx)
        # We can read with balanced = false because it's going to be shuffled and
        # balanced later
        read_block_params["balanced"] = False  # for ReadBlock
        read_block_params["filtering_op"] = lambda unfiltered_df: SplitGroup(
            unfiltered_df, params, 2, 3, comm, "Memory", {}
        )
        # We just pass in 2 and 3 and COMM_WORLD because these parameters
        # don't really matter. We just want to consolidate and get all the data
        # from the partition that actually applies to one of the divisions for this
        # batch.
        # TODO: Pass boundedlower and boundedupper to this

        # Read in data for this batch
        part = ReadBlockFunc(src, read_block_params, 1, 1, comm, loc_name, loc_params)

        # Shuffle the batch and add it to the set of data for this partition
        part = ShuffleFunc(
            part,
            {},
            params,
            comm,
            (not hasdivision) or (batch_idx != firstbatchidx),
            (not hasdivision) or (batch_idx != lastbatchidx),
            False,
        )
        params.pop("divisions_by_partition")

        # Concatenate together the data for this partition
        res = part

        # If there are no divisions for any of the partitions, then they are all
        # bounded. For a partition to be unbounded on one side, there must be a
        # division(s) for that partition.

        # Store divisions
        if not isinstance(res, Empty):
            splitting_divisions = get_splitting_divisions()
            partition_idx = get_partition_idx(batch_idx, nbatches, comm)
            set_in_splitting_divisions(
                res,
                (
                    partition_divisions[partition_idx],
                    (not hasdivision) or (partition_idx != firstdivisionidx),
                    (hasdivision) or (partition_idx != lastdivisionidx),
                ),
            )

        return res

    return ReadGroupHelperFunc


def ReadGroup(ReadGroupHelperFunc):
    def ReadGroupFunc(
        src,
        params: Dict[str, Any],
        batch_idx: int,
        nbatches: int,
        comm: MPI.Comm,
        loc_name: str,
        loc_params: Dict[str, Any],
    ):
        divisions = params["divisions"]
        key = params["key"]
        rev = params.get("rev", False)  # Passed in ReadBlock
        return ReadGroupHelperFunc(
            src,
            params,
            batch_idx,
            nbatches,
            comm,
            loc_name,
            loc_params,
            divisions,
            key,
            rev,
        )

    return ReadGroupFunc


class PartiallyMerged:
    def __init__(self, pieces: list):
        self.pieces = pieces


@dispatch
def SplitBlock(
    src: Union[NoneType, PartiallyMerged],
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    return None


@dispatch
def SplitBlock(
    src: Empty,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    return Empty()


# NOTE: The way we have `partial_merges` requires us to be splitting from
# `nothing` and then merging back. If we are splitting from some value and
# then expecting to merge back in some way then that won't work. If we are
# splitting from a value we assume that we don't have to merge back either
# because we split with a view (so the source was directly mutated) or we
# didn't mutate this value at all. If we are doing in-place mutations where
# we split from some value and then merge back up, then we might have to
# add support for that. Right now, because of the way `SplitBlock`,
# `SplitGroup`, and `Merge` are implemented, we unnecessarily concatenate
# in the case where we are doing things like `setindex!` with a somewhat
# faked mutation.

# src is [] if we are partially merged (because as we iterate over
# batches we take turns between splitting and merging)
@dispatch
def SplitGroup(
    src: Union[NoneType, PartiallyMerged],
    params,
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name,
    loc_params,
    store_splitting_divisions=False,
):
    return None


@dispatch
def SplitGroup(
    src: Empty,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    return Empty()


def MergeHelper(
    src: PartiallyMerged,
    part,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
    splitting_divisions,
    key,
):

    # TODO: To allow for mutation of a value, we may want to remove this
    # condition
    # We only need to concatenate partitions if the source is nothing.
    # Because if the source is something, then part must be a view into it
    # and no data movement is needed.

    # Concatenate across batches
    src.pieces[batch_idx] = part
    if batch_idx == nbatches:
        id_dict_pop(splitting_divisions, part)

        # Concatenate across batches
        to_merge = list(filter(lambda piece: not isinstance(piece, Empty), src.pieces))
        src = Empty() if (len(to_merge) == 0) else merge_on_executor(to_merge, key)
        # src = merge_on_executor(src.pieces; key = key)
        # TODO: Handle case where everything merges to become empty and also ensure WriteHDF5 is correct

        # Concatenate across workers
        nworkers = get_nworkers(comm)
        if nworkers > 1:
            raise Exception("Merging cannot be performed across multiple workers")

    # TODO: Handle Consolidate, Merge, WriteHDF5, WriteJuliaArray, WriteCSV/Parquet/Arrow receiving missing

    return src


@dispatch
def Merge(
    src: Union[NoneType, PartiallyMerged],
    part,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    if get_npartitions(nbatches, comm) == 1:
        src = part
        return src

    if batch_idx == 1:
        P = type(part)
        src = PartiallyMerged([None for _ in range(nbatches)])
    return MergeHelper(
        src,
        part,
        params,
        batch_idx,
        nbatches,
        comm,
        loc_name,
        loc_params,
        get_splitting_divisions(),
        params["key"],
    )


@dispatch
def Merge(
    src,
    part: Any,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    return src


def CopyFrom(
    src,
    params,
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name,
    loc_params,
):
    return src


def CopyFromValue(
    src,
    params,
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name,
    loc_params,
):
    return loc_params["value"]


def CopyFromClient(
    src,
    params,
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name,
    loc_params,
):
    v = loc_params["value_id"]
    received = receive_from_client(v) if get_worker_idx(comm) == 0 else None
    # TODO: Make Replicated not necessarily require it to be replicated _everywhere_
    res = comm.bcast(received, root=0)
    res


def CopyFromPython(
    src,
    params,
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name,
    loc_params,
):
    path = getpath(loc_params["path"])
    try:
        return pickle.load(
            path
        )  # TODO: Do we need to retry desserialization (deserialize_retry(path))
    except:
        # File does not exist
        return None


def CopyTo(
    src,
    part,
    params,
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name,
    loc_params,
):
    src = part


def CopyToClient(
    src,
    part,
    params,
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name,
    loc_params,
):
    if (get_worker_idx(comm) == 0) and (batch_idx == 0):
        send_to_client(loc_params["value_id"], part)


def CopyToPython(
    src,
    part,
    params,
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name,
    loc_params,
):
    if (get_worker_idx(comm) == 0) and (batch_idx == 0):
        return pickle.dump(part, getpath(loc_params["path"]))
    if batch_idx == 0:
        comm.Barrier()


def get_op(params: Dict[str, Any]):
    op = params["reducer"]
    if params["with_key"]:
        key = params["key"]
        if not params.haskey("reducer_with_key"):
            op = op(key)
            reducer_with_key = {key: op}
            params["reducer_with_key"] = reducer_with_key
        else:
            reducer_with_key = params["reducer_with_key"]
            if reducer_with_key.haskey(key):
                op = op(key)
                reducer_with_key[key] = op
            else:
                op = reducer_with_key[key]
    return op


@dispatch
def reduce_in_memory(src: Union[NoneType, Empty], part, op: Callable):
    return part


@dispatch
def reduce_in_memory(src: Union[NoneType, Empty], part: Empty, op: Callable):
    return Empty()


@dispatch
def reduce_in_memory(src, part: Empty, op: Callable):
    return src


@dispatch
def reduce_in_memory(src, part, op: Callable):
    return op(src, part)


def ReduceAndCopyToPython(
    src,
    part,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
    op: Callable,
):
    # Merge reductions from batches
    # TODO: Ensure that we handle reductions that can produce nothing
    src = reduce_in_memory(src, part, op)

    # Merge reductions across workers
    if batch_idx == nbatches:
        src = Reduce(src, params, {}, comm)

        if loc_name != "Memory":
            # We use 1 here so that it is as if we are copying from the head
            # node
            CopyToPython(src, src, params, 1, 1, comm, loc_name, loc_params)

    # TODO: Ensure we don't have issues where with batched execution we are
    # merging to the thing we are splitting from
    # NOTE: We are probably okay for now because we never split and then
    # re-merge new results to the same variable. We always merge to a new
    # variable. But in the future to be more robust, we may want to store
    # partial merges in a global `IdDict` and then only mutate `src` once we
    # are finished with the last batch and we know we won't be splitting
    # from the value again.
    return src


def ReduceAndCopyToPython(
    src,
    part,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    op = get_op(params)
    return ReduceAndCopyToPython(
        src, part, params, batch_idx, nbatches, comm, loc_name, loc_params, op
    )


ReduceWithKeyAndCopyToPython = ReduceAndCopyToPython


def Divide(
    src: range,  # TODO: Ensure this type isn't too limiting
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    return src[split_len(len(src), batch_idx, nbatches, comm)]


def DivideHelper(
    src: tuple,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
    dim: int,
):
    # This is for sizes which are tuples.
    part = src
    # part = CopyFrom(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    newpartdim = len(split_len(part[dim], batch_idx, nbatches, comm))
    return indexapply(newpartdim, part, dim)


def DivideHelper(
    src,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
    dim: int,
):
    part = src
    # part = CopyFrom(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    return len(split_len(part[dim], batch_idx, nbatches, comm))


def Divide(
    src,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    return DivideHelper(
        src, params, batch_idx, nbatches, comm, loc_name, loc_params, params["key"]
    )


def DivideFromValue(
    src,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    part = CopyFromValue(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    return Divide(part, params, batch_idx, nbatches, comm, loc_name, loc_params)


def DivideFromDisk(
    src,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    part = CopyFromPython(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    return Divide(part, params, batch_idx, nbatches, comm, loc_name, loc_params)


def DivideFromClient(
    src,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    part = CopyFromClient(src, params, batch_idx, nbatches, comm, loc_name, loc_params)
    return Divide(part, params, batch_idx, nbatches, comm, loc_name, loc_params)


#####################
# Casting functions #
#####################


def Reduce(
    part, src_params: Dict[str, Any], dst_params: Dict[str, Any], comm: MPI.Comm
):
    # Get operator for reduction
    op = empty_handler(get_op(src_params))

    # TODO: Handle case where different processes have differently sized
    # sendbuf and where sendbuf is not isbitstype

    # Perform reduction
    empty_worker_idx = find_worker_idx_where(isinstance(part, Empty), comm=comm)
    if empty_worker_idx == -1:
        part = reduce_and_sync_across(op, part, comm=comm)
    else:
        gathered = gather_across(part, comm)
        if is_main_worker(comm):
            gathered_nonempty = list(
                filter(lambda piece: not isinstance(piece, Empty), gathered)
            )
            part = sync_across(
                Empty()
                if len(gathered_nonempty) == 0
                else reduce(op, gathered_nonempty),
                comm=comm,
            )
        else:
            part = sync_across(None, comm=comm)
    return part


ReduceWithKey = Reduce


@dispatch
def Distribute(
    part: NoneType,
    src_params: Dict[str, Any],
    dst_params: Dict[str, Any],
    comm: MPI.Comm,
):
    return None


@dispatch
def Distribute(
    part, src_params: Dict[str, Any], dst_params: Dict[str, Any], comm: MPI.Comm
):
    # TODO: Determine whether copy is needed
    return copy(SplitBlock(part, dst_params, 1, 1, comm, "Memory", {}))


def DistributeAndShuffle(
    part: NoneType,
    src_params: Dict[str, Any],
    dst_params: Dict[str, Any],
    comm: MPI.Comm,
):
    return None


def DistributeAndShuffle(
    part, src_params: Dict[str, Any], dst_params: Dict[str, Any], comm: MPI.Comm
):
    SplitGroup(
        part, dst_params, 1, 1, comm, "Memory", {}, store_splitting_divisions=True
    )
