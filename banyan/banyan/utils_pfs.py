from copy import copy, deepcopy
import logging
from math import ceil
from typing import List

try:
    from mpi4py import MPI
except ImportError:
    logging.warning("mpi4py cannot be initialized because MPI is not installed")
import urllib

from .locations import get_remotepath_id
from .session import get_session

####################
# Helper functions #
####################


def get_worker_idx(comm: MPI.Comm = MPI.COMM_WORLD) -> int:
    return MPI.Comm_rank(comm)


def get_nworkers(comm: MPI.Comm = MPI.COMM_WORLD) -> int:
    return MPI.Comm_size(comm)


def is_main_worker(comm: MPI.Comm = MPI.COMM_WORLD) -> bool:
    return get_worker_idx(comm) == 0


def get_partition_idx(
    batch_idx: int, nbatches: int, worker_idx: int = None, comm: MPI.Comm = None
) -> int:
    if worker_idx is None:
        return (worker_idx * nbatches) + batch_idx
    elif comm is not None:
        return get_partition_idx(batch_idx, nbatches, get_worker_idx(comm))
    else:
        raise ValueError("Either worker_idx or comm must be not None")


def get_npartitions(nbatches: int, comm: MPI.Comm) -> int:
    return nbatches * get_nworkers(comm)


def reduce_and_sync_across(func, val, comm=MPI.COMM_WORLD):
    # TODO: Should this be Allreduce?
    return comm.allreduce(val, op=func)


# TODO: Implement this
# def gather_across(obj, comm):
#     is_main = is_main_worker(comm)
#     io = IOBuffer()
#     serialize(io, obj)
#     sendbuf = MPI.Buffer(view(io.data, 1:io.size))
#     sizes = MPI.Gather(sendbuf.count, 0, comm)
#     recvvbuf = is_main ? VBuffer(similar(sendbuf.data, sum(sizes)), sizes) : nothing
#     MPI.Gatherv!(sendbuf, recvvbuf, 0, comm)
#     if is_main
#         [
#             view(
#                 recvvbuf.data,
#                 (recvvbuf.displs[i]+1):(recvvbuf.displs[i]+recvvbuf.counts[i])
#             ) |> IOBuffer |> deserialize
#             for i in 1:get_nworkers(comm)
#         ]
#     else:
#         return []


def split_len_with_fld(src_len: int, idx: int, npartitions: int):
    if npartitions > 1:
        dst_len = src_len // npartitions
        dst_start = min((idx - 1) * dst_len, src_len)
        dst_end = src_len if (idx == npartitions) else min(idx * dst_len, src_len)
        return range(dst_start, dst_end)
    else:
        return range(src_len)


def split_len(src_len: int, idx: int, npartitions: int):
    # TODO: Ensure correct
    if npartitions > 1:
        dst_len = ceil(src_len / npartitions)
        dst_start = min((idx - 1) * dst_len, src_len)
        dst_end = min(idx * dst_len, src_len)
        return range(dst_start, dst_end)
    else:
        return range(src_len)


def reduce_worker_idx_and_val(worker_idx_and_val_a, worker_idx_and_val_b):
    if worker_idx_and_val_a[1]:
        return worker_idx_and_val_a
    else:
        return worker_idx_and_val_b


def find_worker_idx_where(
    val: bool, comm: MPI.Comm = MPI.COMM_WORLD, allreduce: bool = True
) -> int:
    worker_idx = get_worker_idx(comm)
    worker_idx_and_val = (worker_idx, val)
    if allreduce:
        worker_idx, aggregated_val = comm.allreduce(
            worker_idx_and_val, op=reduce_worker_idx_and_val
        )
    else:
        res = comm.reduce(
            worker_idx_and_val, reduce_worker_idx_and_val, get_nworkers(comm) - 1
        )
        if worker_idx == 0:
            worker_idx, aggregated_val = res
        else:
            worker_idx, aggregated_val = -1, False
    return worker_idx if aggregated_val else -1


def divide_division(diff, ndivisionsplits):
    if isinstance(diff, int):
        return ceil(diff / ndivisionsplits)
    elif isinstance(diff, float):
        return diff / ndivisionsplits


def get_divisions(divisions: List[tuple], npartitions: int) -> List[List[tuple]]:
    # This function accepts a list of divisions where each division is a tuple
    # of ordering hashes (values returned by `orderinghash` which are either
    # numbers or vectors of numbers). It also accepts a number of partitions to
    # produce divisions for. The result is a list of length `npartitions`
    # containing lists of divisions for each partition. A partition may contain
    # multiple divisions.

    ndivisions = len(divisions)
    if ndivisions == 0:
        # If there are no divisions (maybe this dataset or this partition of a
        # dataset is empty), we simply return empty set.
        return [[] for _ in range(npartitions)]
    elif ndivisions >= npartitions:
        # If there are more divisions than partitions, we can distribute them
        # easily. Each partition gets 0 or more divisions.
        # TODO: Ensure usage of div here and in sampling (in PT
        # library (here), annotation, and in locations) doesn't result in 0 or
        # instead we use ceiling division
        # ndivisions_per_partition = div(ndivisions, npartitions)
        return list(
            map(
                lambda partition_idx: divisions[
                    split_len(ndivisions, partition_idx, npartitions)
                ],
                range(npartitions),
            )
        )
    else:
        # Otherwise, each division must be shared among 1 or more partitions
        allsplitdivisions = []
        # npartitions_per_division = div(npartitions, ndivisions)

        # Iterate through the divisions and split each of them and find the
        # one that contains a split that this partition must own and use as
        # its `partition_divisions`
        for (division_idx, division) in enumerate(divisions):
            # Determine the range (from `firstpartitioni` to `lastpartitioni`) of
            # partitions that own this division
            # We have to use fld instead of the cld used by split_len because otherwise
            # some divisions might have no partitions! :O
            # partitionsrange = split_len(npartitions, division_idx, ndivisions)
            partitionsrange = split_len_with_fld(npartitions, division_idx, ndivisions)

            # We need to split the division among all the partitions in
            # its range
            ndivisionsplits = len(partitionsrange)

            # Get the `Base.Vector{Number}`s to interpolate between
            divisionbegin = division[0]
            divisionend = division[1]
            T = type(divisionbegin)

            # Initialize divisions for each split
            # V_nonstatic = Base.Vector{T}
            splitdivisions = list(
                map(
                    lambda _: (deepcopy(divisionbegin), deepcopy(divisionend)),
                    range(ndivisionsplits),
                )
            )

            # Adjust the divisions for each split to interpolate. The result
            # of an `orderinghash` call can be an array (in the case of
            # strings), so we must iterate through that array in order to
            # interpolate at the first element in that array where there is a
            # difference.
            for (i, (dbegin, dend)) in enumerate(zip(divisionbegin, divisionend)):
                # Find the first index in the `Base.Vector{Number}` where
                # there is a difference that we can interpolate between
                if dbegin != dend:
                    # Iterate through each split
                    start = copy(dbegin)
                    for j in range(ndivisionsplits):
                        # Update the start and end of the division
                        # islastsplit = j == ndivisionsplits
                        splitdivisions[j][0][i] = dbegin if (j == 1) else copy(start)
                        start += divide_division(dend - dbegin, ndivisionsplits)
                        start = min(start, dend)
                        splitdivisions[j][2][i] = (
                            dend if (j == ndivisionsplits) else copy(start)
                        )

                        # Ensure that the remaining indices are matching between the start and end.
                        if j < ndivisionsplits:
                            splitdivisions[j][1][i + 1 :] = splitdivisions[j][0][
                                i + 1 :
                            ]

                    # Stop if we have found a difference we can
                    # interpolate between
                    # TODO: If the difference is not that much,
                    # interpolate between multiple consecutive
                    # differeing characters together
                    break

            # Each partition must have a _list_ of divisions so we must have a list
            # for each partition. So `allsplitdivisions` is an array where
            # each element is either a 1-element array containing a single
            # division or its empty.
            for splitdivision in splitdivisions:
                # Check if we have already added this split division before.
                # The last split division may have been empty but we can
                # still check whether there is a last one and if what we're
                # adding is the same or also empty. If it is the same or also
                # empty, then we just add an empty divisions list. Otherwsie,
                # we add in our novel split division.
                if (len(allsplitdivisions) != 0) and (
                    allsplitdivisions[-1] == splitdivision
                ):
                    allsplitdivisions = allsplitdivisions.append([])
                else:
                    allsplitdivisions = allsplitdivisions.append(
                        [(splitdivision[0], splitdivision[1])]
                    )

        return allsplitdivisions


def getpath(path):
    if path.startswith("http://") or path.startswith("https://"):
        # TODO: First check for size of file and only download to
        # disk if it doesn't fit in free memory
        # TODO: Add option for Internet locations as to whether or not to
        # cache on disk
        hashed_path = get_remotepath_id(path)
        joined_path = f"efs/job_{get_session().resource_id}_dataset_{hashed_path}_{MPI.Comm_rank(MPI.COMM_WORLD)}"
        # @info "Downloading $path to $joined_path"
        # if MPI.Comm_rank(comm) == 0
        # NOTE: Even though we are storing in /tmp, this is
        # effectively caching the download. If this is undesirable
        # to a user, a short-term solution is to use a different
        # URL each time (e.g., add a dummy query to the end of the
        # URL)
        # TODO: Download to node-local storage and only re-download if the
        # file does not exist (we are scared of checking if files exist because
        # of NFS/S3FS consistency issues)
        urllib.urlretrieve(path, joined_path)
        # end
        # MPI.Barrier(comm)

        return joined_path
    elif path.startswith("s3://"):
        return path.replace("s3://", "/home/ec2-user/s3/")
        # NOTE: We expect that the ParallelCluster instance was set up
        # to have the S3 filesystem mounted at ~/s3fs/<bucket name>
    else:
        # Case of local paths to things stored on disk
        return f"efs/{path}"
