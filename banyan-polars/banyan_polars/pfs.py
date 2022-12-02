from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import logging
from math import ceil
from operator import itemgetter
import os
from shutil import copyfile
from typing import Any, Dict, Union

try:
    from mpi4py import MPI
except ImportError:
    logging.warning("mpi4py cannot be initialized because MPI is not installed")
import polars as pl

from banyan import (
    delete_from_splitting_divisions,
    deserialize_retry,
    Empty,
    ExactSample,
    file_ending,
    from_jl_value_contents,
    get_location_path,
    get_meta_path,
    get_npartitions,
    getpath,
    get_nworkers,
    get_partition_idx,
    get_splitting_divisions,
    get_worker_idx,
    is_main_worker,
    isoverlapping,
    LocationSource,
    read_file,
    rmdir_on_nfs,
    sortablestring,
    SplitBlock,
    split_len,
    sync_across,
    to_jl_value_contents,
    write_file,
)

from arrow import PyArrowTable

symbol_Disk = "Disk"
symbol_filtering_op = "filtering_op"
symbol_path = "path"
symbol_balanced = "balanced"
symbol_nrows = "nrows"
symbol_job_id = "job_id"


def ReadBlockHelper(format_value):
    def ReadBlock(
        src,
        params: Dict[str, Any],
        batch_idx: int,
        nbatches: int,
        comm: MPI.Comm,
        loc_name: str,
        loc_params: Dict[str, Any],
    ) -> pl.DataFrame:
        # TODO: Implement a Read for balanced=false where we can avoid duplicate
        # reading of the same range in different reads

        loc_params_path = loc_params[symbol_path]
        balanced = params[symbol_balanced]
        m_path = (
            sync_across(
                get_meta_path(loc_params_path) if is_main_worker(comm) else "",
                comm=comm,
            )
            if (loc_name == symbol_Disk)
            else loc_params["meta_path"]
        )
        loc_params = (
            (deserialize_retry(get_location_path(loc_params_path))).src_parameters
            if (loc_name == symbol_Disk)
            else loc_params
        )
        meta = PyArrowTable(m_path)
        filtering_op = symbol_filtering_op.get(params, lambda x: x)

        # Handle multi-file tabular datasets

        # time_key = (:reading_lustre) if (loc_name == symbol_Disk) else (:reading_remote)

        # TODO: Use split_across to split up the list of files. Then, read in all the files and concatenate them.
        # Finally, shuffle by sending to eahc
        # TODO: use path and nrows
        # Use first-fit-decreasing bin-packing [1] to assign files to workers
        # [1] https://en.wikipedia.org/wiki/First-fit-decreasing_bin_packing

        # Initialize
        meta_nrows = meta.nrows
        meta_path = meta.path
        nworkers = get_nworkers(comm)
        npartitions = nbatches * nworkers
        partition_idx = get_partition_idx(batch_idx, nbatches, comm)
        nrows = loc_params[symbol_nrows]
        rows_per_partition = ceil(nrows / npartitions)
        sorting_perm = [
            x for x, y in sorted(enumerate(meta_nrows), reverse=True, key=itemgetter(1))
        ]
        files_by_partition = []
        nrows_by_partition = [0 for _ in range(npartitions)]
        for _ in range(npartitions):
            files_by_partition = files_by_partition.append([])

        # Try to fit as many files as possible into each partition and keep
        # track of the files that are too big
        too_large_files = []
        for file_i in sorting_perm:
            too_large_file = True
            for (i, (files, nrows)) in enumerate(
                zip(files_by_partition, nrows_by_partition)
            ):
                file_nrows = meta_nrows[file_i]
                if (nrows + file_nrows) <= rows_per_partition:
                    files = files.append(file_i)
                    nrows_by_partition[i] += file_nrows
                    too_large_file = False  # it actually fits!!
                    break

            if too_large_file:
                too_large_files = too_large_files.append(file_i)

        # Fit in the files that are too large by first only using partitions
        # that haven't yet been assigned any rows. Prioritize earlier batches.
        second_pass = False
        while len(too_large_files) != 0:
            # On the first pass, we try to fit into partitions not yet assigned any file
            # On the second pass, we just assign stuff anywhere
            for batch_i in range(nbatches):
                for worker_i in range(nworkers):
                    curr_partition_idx = get_partition_idx(batch_i, nbatches, worker_i)
                    if (
                        nrows_by_partition[curr_partition_idx] == 0 or second_pass
                    ) and (len(too_large_files) != 0):
                        file_i = too_large_files.pop(0)
                        files_by_partition[curr_partition_idx] = files_by_partition[
                            curr_partition_idx
                        ].append(file_i)
                        nrows_by_partition[curr_partition_idx] += meta_nrows[file_i]
            second_pass = True

        # Read in data frames
        if not balanced:
            files_for_curr_partition = files_by_partition[partition_idx]
            if len(files_for_curr_partition) != 0:
                dfs_res = [None for _ in range(len(files_for_curr_partition))]

                def reader(dfs_res, meta_path, format_value, i, file_i):
                    path = getpath(meta_path[file_i])
                    dfs_res[i] = filtering_op(read_file(format_value, path))

                with ThreadPoolExecutor() as executor:
                    future_to_params = {
                        executor.submit(
                            reader, dfs_res, meta_path, format_value, i, file_i
                        ): (i, file_i)
                        for (i, file_i) in list(enumerate(files_for_curr_partition))
                    }
                dfs = dfs_res
            else:
                dfs = []
        else:
            # Determine the range of rows to read in from each file so that the result
            # is perfectly balanced across all partitions
            rowrange = split_len(nrows, batch_idx, nbatches, comm)
            ndfs = 0
            rowsscanned = 0
            files_to_read = []
            for file in Tables.rows(meta):
                path = file[1]
                path_nrows = file[2]
                newrowsscanned = rowsscanned + path_nrows
                filerowrange = range(rowsscanned, newrowsscanned)

                # Check if the file corresponds to the range of rows for the batch
                # currently being processed by this worker
                if isoverlapping(filerowrange, rowrange):
                    ndfs += 1
                    readrange = range(
                        max(rowrange.start, filerowrange.start),
                        min(rowrange.stop, filerowrange.stop),
                    )
                    files_to_read = files_to_read.append(
                        (ndfs, path, readrange, filerowrange)
                    )
                rowsscanned = newrowsscanned
            dfs = [None for _ in range(ndfs)]

            # Iterate through files and identify which ones correspond to the range of
            # rows for the batch currently being processed by this worker
            rowrange = split_len(nrows, batch_idx, nbatches, comm)
            rowsscanned = 0

            def reader(dfs, format_value, path, rowrange, readrange, filerowrange):
                dfs[i] = read_file(
                    format_value, getpath(path), rowrange, readrange, filerowrange
                )

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_params = {
                    executor.submit(
                        reader, format_value, path, rowrange, readrange, filerowrange
                    ): (i, path, readrange, filerowrange)
                    for (i, path, readrange, filerowrange) in files_to_read
                }

        # Concatenate and return
        # NOTE: If this partition is empty, it is possible that the result is
        # schemaless (unlike the case with HDF5 where the resulting array is
        # guaranteed to have its ndims correct) and so if a split/merge/cast
        # function requires the schema (for example for grouping) then it must be
        # sure to take that account
        if all([df is None for df in dfs]):
            # When we construct the location, we store an empty data frame with The
            # correct schema.
            return from_jl_value_contents(loc_params["empty_sample"])
        elif len(dfs) == 1:
            return dfs[0]
        else:
            return pl.concat(dfs)

    return ReadBlock


# We currently don't expect to ever have Empty dataframes. We only expect Empty arrays
# and values resulting from mapslices or reduce. If we do have Empty dataframes arising
# that can't just be empty `DataFrame()`, then we will modify functions in this file to
# support Empty inputs.


def WriteHelper(format_value):
    def Write(
        src,
        part: Union[pl.DataFrame, Empty],
        params: Dict[str, Any],
        batch_idx: int,
        nbatches: int,
        comm: MPI.Comm,
        loc_name: str,
        loc_params: Dict[str, Any],
    ):
        # Get rid of splitting divisions if they were used to split this data into
        # groups

        delete_from_splitting_divisions(part)

        ###########
        # Writing #
        ###########

        # Get path of directory to write to
        is_disk = loc_name == "Disk"
        loc_params_path = loc_params["path"]
        path = loc_params_path
        if path.startswith("http://") or path.startswith("https://"):
            raise Exception("Writing to http(s):// is not supported")
        elif path.startswith("s3://"):
            path = getpath(path)
            # NOTE: We expect that the ParallelCluster instance was set up
            # to have the S3 filesystem mounted at ~/s3fs/<bucket name>
        else:
            # Prepend "efs/" for local paths
            path = getpath(path)

        # Write file for this partition
        worker_idx = get_worker_idx(comm)
        is_main = worker_idx == 1
        nworkers = get_nworkers(comm)
        idx = get_partition_idx(batch_idx, nbatches, comm)
        actualpath = deepcopy(path)
        format_string = file_ending(format_value)
        if nbatches > 1:
            # Add _tmp to the end of the path
            path = (
                f"{path}_tmp"
                if (loc_name == "Disk")
                else path.replace(f".{format_string}", f"_tmp.{format}_string")
            )

        # TODO: Delete existing files that might be in the directory but first
        # finish writing to a path*"_new" directory and then linking ... or
        # something like that. Basically we need to handle the case where we
        # have batching in the first PT and we are reading from and writing to
        # the same directory.
        # 1. Write all output to a new directory
        # 2. On the last partition, do a barrier (or a gather) and then delete
        # the old directory
        # 3. Do another barrier on the last batch and delete the old directory
        # and link to the new one

        # NOTE: This is only needed because we might be writing to the same
        # place we are reading from. And so we want to make sure we finish
        # reading before we write the last batch
        if batch_idx == nbatches:
            comm.Barrier()

        if is_main:
            if nbatches == 1:
                # If there is no batching we can delete the original directory
                # right away. Otherwise, we must delete the original directory
                # only at the end.
                # TODO: When refactoring the locations, think about how to allow
                # stuff in the directory
                rmdir_on_nfs(actualpath)

            # Create directory if it doesn't exist
            # TODO: Avoid this and other filesystem operations that would be costly
            # since S3FS is being used
            if batch_idx == 1:
                rmdir_on_nfs(path)
                os.mkdir(path)
        comm.Barrier()

        # Write out for this batch
        nrows = 0 if isinstance(part, Empty) else size(part, 1)
        npartitions = get_npartitions(nbatches, comm)
        sortableidx = sortablestring(idx, npartitions)
        part_res = (
            part if isinstance(part, Empty) else convert(DataFrames.DataFrame, part)
        )
        if not isinstance(part, Empty):
            dst = os.path.join(path, f"part_{sortableidx}.{format_string}")
            write_file(format_value, part_res, dst, nrows)
        # We don't need this barrier anymore because we do a broadcast right after
        # MPI.Barrier(comm)

        ##########################################
        # SAMPLE/METADATA COLLECTIOM AND STORAGE #
        ##########################################

        # Get paths for reading in metadata and Location
        tmp_suffix = ".tmp" if (nbatches > 1) else ""
        m_path = get_meta_path(loc_params_path + tmp_suffix) if is_main else ""
        location_path = (
            get_location_path(loc_params_path + tmp_suffix) if is_main else ""
        )
        m_path, location_path = sync_across((m_path, location_path), comm=comm)

        # Read in meta path if it's there
        if (nbatches > 1) and (batch_idx > 1):
            curr_meta = PyArrowTable(m_path)
            curr_remotepaths, curr_nrows = curr_meta[:path], curr_meta[:nrows]
        else:
            curr_remotepaths, curr_nrows = [], []

        # Read in the current location if it's there
        empty_df = pl.DataFrame()
        if (nbatches > 1) and (batch_idx > 1):
            curr_location = deserialize_retry(location_path)
        else:
            curr_location = LocationSource(
                "Remote",
                {
                    "format": format_string,
                    "nrows": 0,
                    "path": loc_params_path,
                    "meta_path": m_path,
                    "empty_sample": to_jl_value_contents(empty_df),
                },
                0,
                ExactSample(empty_df, 0),
            )

        # Gather # of rows, # of bytes, empty sample, and actual sample
        nbytes = 0 if isinstance(part_res, Empty) else total_memory_usage(part_res)
        sample_rate = get_session().sample_rate
        sampled_part = (
            empty_df
            if (isinstance(part_res, Empty) or is_disk)
            else get_sample_from_data(part_res, sample_rate, nrows)
        )
        gathered_data = gather_across(
            (
                nrows,
                nbytes,
                part_res if isinstance(part_res, Empty) else empty(part_res),
                sampled_part,
            ),
            comm,
        )

        # On the main worker, finalize metadata and location info.
        if is_main:
            # Determine paths and #s of rows for metadata file
            for worker_i in range(nworkers):
                sortableidx = sortablestring(
                    get_partition_idx(batch_idx, nbatches, worker_i), npartitions
                )
                curr_remotepaths = curr_remotepaths.append(
                    os.path.join(loc_params_path, f"part_{sortableidx}.{format_string}")
                )

            # Update the # of bytes
            total_nrows = curr_location.src_parameters["nrows"]
            empty_sample_found = False
            for (new_nrows, new_nbytes, empty_part, sampled_part) in gathered_data:
                # Update the total # of rows and the total # of bytes
                total_nrows += sum(new_nrows)
                curr_nrows = curr_nrows.append(new_nrows)
                curr_location.total_memory_usage += new_nbytes

                # Get the empty sample
                if (not empty_sample_found) and (not isinstance(empty_part, Empty)):
                    curr_location.src_parameters["empty_sample"] = to_jl_value_contents(
                        empty_part
                    )
                    empty_sample_found = True
            curr_location.src_parameters["nrows"] = total_nrows

            # Get the actual sample by concatenating
            if is_disk:
                curr_location.sample = Sample()
            else:
                sampled_parts = [gathered[3] for gathered in gathered_data]
                if batch_idx > 1:
                    sampled_parts = sampled_parts.append(
                        DataFrames.DataFrame(
                            Arrow.Table(seekstart(curr_location.sample.value))
                        )
                    )
                new_sample_value_arrow = IOBuffer()
                Arrow.write(
                    new_sample_value_arrow, pl.concat(sampled_parts), compress="zstd"
                )
                curr_location.sample = Sample(
                    new_sample_value_arrow, curr_location.total_memory_usage
                )

            # Determine paths for this batch and gather # of rows
            Arrow.write(
                m_path, path=curr_remotepaths, nrows=curr_nrows, compress="zstd"
            )

            if (
                (not is_disk)
                and (batch_idx == nbatches)
                and (total_nrows <= get_max_exact_sample_length())
            ):
                # If the total # of rows turns out to be inexact then we can simply mark it as
                # stale so that it can be collected more efficiently later on
                # We should be able to quickly recompute a more useful sample later
                # on when we need to use this location.
                curr_location.sample_invalid = True

            # Write out the updated `Location`
            serialize(location_path, curr_location)

        ###################################
        # Handling Final Batch by Copying #
        ###################################

        if (nbatches > 1) and (batch_idx == nbatches):
            # Copy over location and meta path
            actual_meta_path = get_meta_path(loc_params_path)
            actual_location_path = get_location_path(loc_params_path)
            if worker_idx == 1:
                copyfile(m_path, actual_meta_path)
                copyfile(location_path, actual_location_path)

            # Copy over files to actual location
            tmpdir = readdir(path)
            if is_main:
                rmdir_on_nfs(actualpath)
                os.mkdir(actualpath)
            comm.Barrier()
            for batch_i in range(nbatches):
                idx = get_partition_idx(batch_i, nbatches, worker_idx)
                sortableidx = sortablestring(idx, get_npartitions(nbatches, comm))
                tmpdir_idx = -1
                for i in range(len(tmpdir)):
                    if f"part_{sortableidx}" in tmpdir[i]:
                        tmpdir_idx = i
                        break
                if tmpdir_idx != -1:
                    tmpsrc = os.path.join(path, tmpdir[tmpdir_idx])
                    actualdst = os.path.join(actualpath, tmpdir[tmpdir_idx])
                    copyfile(tmpsrc, actualdst)
            comm.Barrier()
            if is_main:
                rmdir_on_nfs(path)
        else:
            comm.Barrier()

        return None
        # TODO: Delete all other part* files for this value if others exist

    return Write


@dispatch
def SplitBlock(
    src: pl.DataFrame,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    split_on_executor(
        src,
        1,
        batch_idx,
        nbatches,
        comm,
    )


symbol_key = "key"
symbol_rev = "rev"
symbol_consolidate = "consolidate"
symbol_divisions = "divisions"
symbol_boundedlower = "boundedlower"
symbol_boundedupper = "boundedupper"


# It's only with BanyanDataFrames to we have block-partitioned things that can
# be merged to become nothing.
# Grouped data frames can be block-partitioned but we will have to
# redo the groupby if we try to do any sort of merging/splitting on it.

@dispatch
def RebalanceDataFrame(
    part: pl.DataFrame,
    src_params: Dict[str, Any],
    dst_params: Dict[str, Any],
    comm: MPI.Comm
):
    # Get the range owned by this worker
    dim = 1
    worker_idx, nworkers = get_worker_idx(comm), get_nworkers(comm)
    len = size(part, dim)
    scannedstartidx = comm.Exscan(len, op=MPI.Sum)
    startidx = 1 if (worker_idx == 0) else scannedstartidx + 1
    endidx = startidx + len - 1

    # Get functions for serializing/deserializing
    # TODO: Use JLD for ser/de for arrays
    # TODO: Ensure that we are properly handling intermediate arrays or
    # dataframes that are empty (especially because they may not have their
    # ndims or dtype or schema). We probably are because dataframes that are
    # empty should concatenate properly. We just need to be sure to not expect
    # every partition to know what its schema is. We can however expect each
    # partition of an array to know its ndims.

    # Construct buffer to send parts to all workers who own in this range
    nworkers = get_nworkers(comm)
    npartitions = nworkers
    whole_len = MPI.bcast(endidx, nworkers - 1, comm)
    io = IOBuffer()
    nbyteswritten = 0
    counts = []
    for partition_idx in range(npartitions):
        # `Banyan.split_len` gives us the range that this partition needs
        partitionrange = split_len(whole_len, partition_idx, npartitions)

        # Check if the range overlaps with the range owned by this worker
        rangesoverlap = (max(startidx, partitionrange.start) <= min(endidx, partitionrange.stop))

        # If they do overlap, then serialize the overlapping slice
        Arrow.write(
            io,
            view(
                part,
                Base.fill(:, dim - 1)...,
                if rangesoverlap
                    max(1, partitionrange.start - startidx + 1):min(
                        size(part, dim),
                        partitionrange.stop - startidx + 1,
                    )
                else
                    # Return zero length for this dimension
                    1:0
                end,
                Base.fill(:, ndims(part) - dim)...,
            ),
            compress=:zstd
        )

        # Add the count of the size of this chunk in bytes
        counts = counts.append(io.size - nbyteswritten)
        nbyteswritten = io.size
    sendbuf = MPI.VBuffer(view(io.data, 1:nbyteswritten), counts)

    # Create buffer for receiving pieces
    # TODO: Refactor the intermediate part starting from there if we add
    # more cases for this function
    sizes = MPI.Alltoall(MPI.UBuffer(counts, 1), comm)
    recvbuf = MPI.VBuffer(similar(io.data, sum(sizes)), sizes)

    # Perform the shuffle
    comm.Alltoallv(sendbuf, recvbuf)

    # Return the concatenated array
    things_to_concatenate = [
        de(view(recvbuf.data, displ+1:displ+count)) for
        (displ, count) in zip(recvbuf.displs, recvbuf.counts)
    ]
    res = merge_on_executor(
        things_to_concatenate,
        dim,
    )
    res
end

def RebalanceDataFrame(
    part: Empty,
    src_params: Dict[str, Any],
    dst_params: Dict[str, Any],
    comm:MPI.Comm
):
    return RebalanceDataFrame(
        pl.DataFrame(),
        src_params,
        dst_params,
        comm
    )

# If this is a grouped data frame or nothing (the result of merging
# a grouped data frame is nothing), we consolidate by simply returning
# nothing.

def ConsolidateDataFrame(part: pl.DataFrame, src_params: Dict[str, Any], dst_params: Dict[str, Any], comm:MPI.Comm):
    io = IOBuffer()
    Arrow.write(io, part, compress="lz4")
    sendbuf = MPI.Buffer(view(io.data, 1:io.size))
    recvvbuf = buftovbuf(sendbuf, comm)
    comm.Gatherv(sendbuf, recvvbuf)
    if is_main_worker(comm):
        res = merge_on_executor(
            [
                de(view(
                    recvvbuf.data,
                    (recvvbuf.displs[i]+1):(recvvbuf.displs[i]+recvvbuf.counts[i])
                ))
                for i in 1:Banyan.get_nworkers(comm)
            ],
            1
        )
    else:
        res = empty(part)
    return res

def ConsolidateDataFrame(
    part: Empty,
    src_params: Dict[str, Any],
    dst_params: Dict[str, Any],
    comm:MPI.Comm
):
    return ConsolidateDataFrame(
        pl.DataFrame(),
        src_params,
        dst_params,
        comm
    )

# function combine_in_memory(a, b, groupcols, groupkwargs, combinecols, combineargs, combinekwargs)
#     concatenated = vcat(a, b)
#     grouped = groupby(concatenated, groupcols; groupkwargs...)
#     combined = combine(grouped, combineargs...; combinekwargs...)
#     combined
# end

@dispatch
def ReduceDataFrame(
    part,
    src_params: Dict[str, Any],
    dst_params: Dict[str, Any],
    comm: MPI.Comm
):
    res = ConsolidateDataFrame(part, {}, {}, comm)
    res = src_params["reducing_op"](res, pl.DataFrame())
    res_finished = src_params["finishing_op"](res)
    return res_finished

@dispatch
def ReduceDataFrame(part: Empty, src_params: Dict[str, Any], dst_params: Dict[str, Any], comm: MPI.Comm):
    return ReduceDataFrame(pl.DataFrame(), src_params, dst_params, comm)

def ReduceAndCopyToArrow(
    src,
    part,
    params: Dict[str, Any],
    batch_idx: int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
    start_op: Callable,
    reduce_op: Callable,
    finish_op: Callable
):
    # Get the # of rows of each group if this is a group-by-mean that is being reduced
    if loc_name == "Memory":
        part = start_op(part)

    # Concatenate all data frames on this worker
    if nbatches > 1:
        src = Merge(
            src,
            part,
            params,
            batch_idx,
            nbatches,
            MPI.COMM_SELF,
            loc_name,
            loc_params
        )
    else:
        src = part

    # Merge reductions across workers
    if batch_idx == nbatches:
        # TODO: Eliminate the redundant concatenation with empty data frames that the reduce_op
        # performs for group-by aggregation
        src = reduce_op(src, pl.DataFrame())

        if get_nworkers(comm) > 1:
            src = ConsolidateDataFrame(src, {}, {}, comm)
            src = reduce_op(src, pl.DataFrame())
            # NOTE: We use the above to eliminate the compilation overhead of creating
            # custom MPI reductions
            # src = reduce_across(reduce_op, src, comm=comm)

        if loc_name != "Memory":
            if is_main_worker(comm):
                src = finish_op(src)
            else:
                src = pl.DataFrame()
            return CopyToArrow(src, src, params, 1, 1, comm, loc_name, loc_params)

    return src

def ReduceAndCopyToArrow(
    src,
    part,
    params: Dict[str, Any],
    batch_idx:int,
    nbatches: int,
    comm: MPI.Comm,
    loc_name: str,
    loc_params: Dict[str, Any],
):
    return ReduceAndCopyToArrow(
        pl.DataFrame() if isinstance(str, Empty) else src,
        part,
        params,
        batch_idx,
        nbatches,
        comm,
        loc_name,
        loc_params,
        params["starting_op"],
        params["reducing_op"],
        params["finishing_op"]
    )
