from math import ceil

from mpi4py import MPI

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


def reduce_and_sync_across():
    # TODO: Implement
    pass


def gather_across():
    # TODO: Implement
    pass


def split_len(src_len: int, idx: int, npartitions: int):
    # TODO: Ensure correct
    if npartitions > 1:
        dst_len = ceil(src_len / npartitions)
        dst_start = min((idx - 1) * dst_len, src_len)
        dst_end = min(idx * dst_len, src_len)
        return range(dst_start, dst_end)
    else:
        return range(src_len)


def find_worker_idx_where():
    # TODO: Implement
    pass


def get_divisions():
    # TODO: Implement
    pass


def getpath():
    # TODO: Implement
    pass
