import itertools
from math import ceil
import os
import random

from banyan import (
    cache_location,
    ExactSample,
    gather_across,
    getpath,
    get_sample_and_metadata,
    get_cached_location,
    get_max_exact_sample_length,
    get_metadata,
    get_meta_path,
    get_sample,
    get_session,
    has_separate_metadata,
    is_main_worker,
    Location,
    LocationDestination,
    LocationSource,
    NOTHING_LOCATION,
    NOTHING_SAMPLE,
    offloaded,
    reduce_and_sync_across,
    Sample,
    split_across,
    sync_across,
    to_jl_value_contents,
    total_memory_usage,
)

from mpi4py import MPI
import polars as pl
import pyarrow as pa

from arrow import PyArrowTable


def get_file_ending(remotepath: str) -> str:
    return remotepath.split(".")[-1]


def _remote_table_source(
    remotepath,
    shuffled,
    metadata_invalid,
    sample_invalid,
    invalidate_metadata,
    invalidate_sample,
    max_exact_sample_length,
) -> Location:
    session_sample_rate = get_session().sample_rate
    is_main = is_main_worker()

    # Get cached Location and if it has valid parameters and sample, return
    curr_location, curr_sample_invalid, curr_parameters_invalid = get_cached_location(
        remotepath, metadata_invalid, sample_invalid
    )
    if (not curr_parameters_invalid) and (not curr_sample_invalid):
        return curr_location

    # There are two things we cache for each call `to _remote_table_source`:
    # 1. A `Location` serialized to a `location_path`
    # 2. Metadata stored in an Arrow file at `meta_path`

    # Get metadata if it is still valid (read using pyarrow)
    curr_meta: pa.Table = None
    if not curr_parameters_invalid:
        # TODO: Do we need to retry here?
        curr_meta = PyArrowTable(curr_location.src_parameters["meta_path"])
    else:
        curr_meta = pa.Table.from_pydict({})

    # Metadata file structure
    # - Arrow file
    # - Constructed with a `NamedTuple` mapping to `Vector`s and read in as an Arrow.Table
    # - Columns: file name, # of rows, # of bytes

    # Get list of local paths. Note that in the future when we support a list of
    # Internet locations, we will want to only call getpath laterin this code when/if
    # we actually read stuff in.
    if not curr_parameters_invalid:
        remotepaths_res = curr_meta["path"]
        localpaths, remotepaths = list(map(getpath, remotepaths_res)), remotepaths_res
    else:
        localpath = getpath(remotepath)
        localpath_is_dir = os.isdir(localpath)
        if localpath_is_dir:
            paths_on_main = os.listdir(localpath) if is_main else []
            paths = sync_across(paths_on_main)
            npaths = len(paths)
            localpaths_res = [None for _ in range(npaths)]
            remotepaths_res = [None for _ in range(npaths)]
            for i in range(npaths):
                localpaths_res[i] = os.path.join(localpath, paths[i])
                remotepaths_res[i] = os.path.join(remotepath, paths[i])
            localpaths, remotepaths = localpaths_res, remotepaths_res
        else:
            localpaths, remotepaths = [localpath], [remotepath]
    curr_meta_nrows = curr_meta["nrows"] if not curr_parameters_invalid else []
    local_paths_on_curr_worker = split_across(localpaths)

    # Get format
    format_string = get_file_ending(remotepath)
    format_value = globals()[format_string.capitalize()]
    format_has_separate_metadata = has_separate_metadata(format_value)

    # Get nrows, nbytes for each file in local_paths_on_curr_worker
    if curr_parameters_invalid:
        meta_nrows_on_worker_res = [0 for _ in range(len(local_paths_on_curr_worker))]
        if format_has_separate_metadata:
            for i, local_path_on_curr_worker in enumerate(local_paths_on_curr_worker):
                path_nrows_on_worker = get_metadata(
                    format_value, local_path_on_curr_worker
                )
                meta_nrows_on_worker_res[i] = path_nrows_on_worker
        # If this format doesn't have separate metadata, we will have to
        # read it in later along with the sample itself.
        meta_nrows_on_worker = meta_nrows_on_worker_res
    else:
        meta_nrows_on_worker = split_across(curr_meta_nrows)

    # Compute the total # of rows so that if the current sample is invalid
    # we can determine whether to get an exact or inexact sample and
    # otherwise so that we can update the sample rate.
    if curr_parameters_invalid:
        # For formats with metadata stored with the data (CSV), we
        # determine the # of rows later in the below case where
        # `!is_metadata_valid``.
        total_nrows_res = (
            reduce_and_sync_across(MPI.SUM, sum(meta_nrows_on_worker))
            if format_has_separate_metadata
            else -1
        )
    else:
        total_nrows_res = curr_location.src_parameters["nrows"]
    exact_sample_needed = total_nrows_res < max_exact_sample_length

    # inv: (a) `meta_nrows_on_worker`, (b) `total_nrows_res`, and
    # (c) `exact_sample_needed` are only valid if either the format has
    # separate metadata (like Parquet and Arrow) or the metadata is already
    # stored and valid.
    is_metadata_valid = format_has_separate_metadata or (not curr_parameters_invalid)
    # If the metadata isn't valid then we anyway have to read in all the data
    # so we can't leverage the data being shuffled by only reading in some of the files
    shuffled = shuffled and is_metadata_valid and (not exact_sample_needed)

    # Get sample and also metadata if not yet valid at this point
    recollected_sample_needed = curr_sample_invalid or (not is_metadata_valid)
    if recollected_sample_needed:
        # In this case, we actually recollect a sample. This is the case
        # where either we actually have an invalid sample or the sample is
        # valid but the metadata is changed and the format is such that
        # recollecting metadata information would be more expensive than
        # any recollection of sample.

        # Get local sample
        local_samples = []
        if is_metadata_valid:
            # Determine which files to read from if shuffled
            if shuffled:
                perm_for_shuffling = random.shuffle(
                    list(range(len(meta_nrows_on_worker)))
                )
                shuffled_meta_nrows_on_worker = meta_nrows_on_worker[perm_for_shuffling]
                nrows_on_worker_so_far = 0
                nrows_on_worker_target = ceil(
                    sum(meta_nrows_on_worker) / session_sample_rate
                )
                nfiles_on_worker_res = 0
                for nrows_on_worker in shuffled_meta_nrows_on_worker:
                    nrows_on_worker_so_far += nrows_on_worker
                    nfiles_on_worker_res += 1
                    if nrows_on_worker_so_far >= nrows_on_worker_target:
                        break
                shuffling_perm, nfiles_on_worker, nrows_extra_on_worker = (
                    perm_for_shuffling,
                    nfiles_on_worker_res,
                    nrows_on_worker_so_far - nrows_on_worker_target,
                )
            else:
                shuffling_perm, nfiles_on_worker, nrows_extra_on_worker = (
                    Colon(),
                    len(local_paths_on_curr_worker),
                    0,
                )
            meta_nrows_for_worker = meta_nrows_on_worker[shuffling_perm]

            # Get local sample
            for (i, local_path_on_curr_worker) in zip(
                list(range(nfiles_on_worker)),
                local_paths_on_curr_worker[shuffling_perm],
            ):
                df = get_sample(
                    format_value,
                    local_path_on_curr_worker,
                    1.0 if (shuffled or exact_sample_needed) else session_sample_rate,
                    meta_nrows_for_worker[i],
                )
                local_samples = local_samples.append(
                    df[1 : (end - nrows_extra_on_worker), :]
                    if (
                        shuffled
                        and (i == nfiles_on_worker)
                        and (nrows_extra_on_worker > 0)
                    )
                    else df
                )
        else:
            # This is the case for formats like CSV where we must read in the
            # metadata with the data AND the metadata is stale and couldn't
            # just have been read from the Arrow metadata file.

            local_nrows = 0
            for exact_sample_needed_res in [False, True]:
                # First see if we can get a random (inexact sample).
                local_samples = []
                local_nrows = 0
                for (i, local_path_on_curr_worker) in enumerate(
                    local_paths_on_curr_worker
                ):
                    path_sample, path_nrows = get_sample_and_metadata(
                        format_value,
                        local_path_on_curr_worker,
                        1.0 if exact_sample_needed_res else session_sample_rate,
                    )
                    meta_nrows_on_worker[i] = path_nrows
                    local_samples = local_samples.append(path_sample)
                    local_nrows += path_nrows
                total_nrows_res = reduce_and_sync_across(MPI.SUM, local_nrows)

                # If the sample is too small, redo it, getting an exact sample
                if (not exact_sample_needed_res) and (
                    total_nrows_res < max_exact_sample_length
                ):
                    exact_sample_needed = True
                    exact_sample_needed_res = True
                else:
                    exact_sample_needed = False
                    break

        local_sample = (
            pl.DataFrame() if (len(local_samples) == 0) else pl.concat(local_samples)
        )

        # Concatenate local samples and nrows together
        if curr_parameters_invalid:
            sample_and_meta_nrows_per_worker = gather_across(
                (local_sample, meta_nrows_on_worker)
            )
            if is_main:
                sample_per_worker = []
                meta_nrows_per_worker = []
                for sample_and_meta_nrows in sample_and_meta_nrows_per_worker:
                    sample_per_worker = sample_per_worker.append(
                        sample_and_meta_nrows[0]
                    )
                    meta_nrows_per_worker = meta_nrows_per_worker.append(
                        sample_and_meta_nrows[1]
                    )
                remote_sample_value, meta_nrows_on_workers = pl.concat(
                    sample_per_worker
                ), list(itertools.chain.from_iterable(meta_nrows_per_worker))
            else:
                remote_sample_value, meta_nrows_on_workers = pl.DataFrame(), []
        else:
            sample_per_worker = gather_across(local_sample)
            if is_main and (len(sample_per_worker) != 0):
                remote_sample_value, meta_nrows_on_workers = (
                    pl.concat(sample_per_worker),
                    curr_meta_nrows,
                )
            else:
                remote_sample_value, meta_nrows_on_workers = pl.DataFrame(), []

        # At this point the metadata is valid regardless of whether this
        # format has metadata stored separately or not. We have a valid
        # (a) `meta_nrows_on_worker`, (b) `total_nrows_res`, and
        # (c) `exact_sample_needed`.
        is_metadata_valid = True

        # Return final Sample on main worker now that we have gathered both the sample and metadata
        if is_main:
            empty_sample_value_serialized = to_jl_value_contents(
                []
            )  # empty(remote_sample_value)

            # Convert dataframe to a buffer storing Arrow-serialized data.
            # Then when we receive this on the client side we can simply
            # parse it back into a data frame. This is just to achieve lower
            # latency for retrieving metadata/samples for BDF.jl.
            io = IOBuffer()
            Arrow.write(io, remote_sample_value, compress="zstd")
            remote_sample_value_arrow = io

            # Construct Sample with the concatenated value, memory usage, and sample rate
            remote_sample_value_memory_usage = total_memory_usage(remote_sample_value)
            if exact_sample_needed:
                total_nbytes_res = remote_sample_value_memory_usage
            else:
                total_nbytes_res = ceil(
                    remote_sample_value_memory_usage * session_sample_rate
                )
            remote_sample_value_nrows = remote_sample_value.nrows
            if exact_sample_needed:
                # Technically we don't need to be passing in `total_bytes_res`
                # here but we do it because we are anyway computing it to
                # return as the `total_memory_usage` for the `Location` and so
                # we might as well avoid recomputing it in the `Sample`
                # constructors
                remote_sample_res = ExactSample(
                    remote_sample_value_arrow, total_nbytes_res
                )
            else:
                remote_sample_res = Sample(remote_sample_value_arrow, total_nbytes_res)
            meta_nrows, total_nrows, total_nbytes, remote_sample, empty_sample = (
                meta_nrows_on_workers,
                total_nrows_res,
                total_nbytes_res,
                remote_sample_res,
                empty_sample_value_serialized,
            )
        else:
            meta_nrows, total_nrows, total_nbytes, remote_sample, empty_sample = (
                [0 for _ in range(len(localpaths))],
                -1,
                -1,
                NOTHING_SAMPLE,
                to_jl_value_contents(pl.DataFrame()),
            )
    else:
        # This case is entered if we the format has metadata stored
        # separately and we only had to recollect the metadata and could
        # avoid recollecting the sample as we would in the other case.

        # inv: is_metadata_valid == true

        # If the sample is valid, the metadata must be invalid and need concatenation.
        meta_nrows_per_worker = gather_across(meta_nrows_on_worker)
        if is_main:
            meta_nrows_res = list(itertools.chain.from_iterable(meta_nrows_per_worker))

            # Get the total # of bytes
            cached_remote_sample_res = curr_location.sample
            remote_sample_value_nrows = cached_remote_sample_res.value.nrows
            remote_sample_value_nbytes = total_memory_usage(
                cached_remote_sample_res.value
            )
            total_nbytes_res = ceil(
                remote_sample_value_nbytes * total_nrows_res / remote_sample_value_nrows
            )

            # Update the sample's sample rate and memory usage based on the
            # new # of rows (since the metadata with info about # of rows
            # has been invalidated)
            cached_remote_sample_res.rate = ceil(
                total_nrows_res / remote_sample_value_nrows
            )
            cached_remote_sample_res.memory_usage = ceil(
                total_nbytes_res / cached_remote_sample_res.rate
            )

            meta_nrows, total_nrows, total_nbytes, remote_sample, empty_sample = (
                meta_nrows_res,
                total_nrows_res,
                total_nbytes_res,
                cached_remote_sample_res,
                curr_location.src_parameters["empty_sample"],
            )
        else:
            meta_nrows, total_nrows, total_nbytes, remote_sample, empty_sample = (
                [0 for _ in range(len(localpaths))],
                -1,
                -1,
                NOTHING_SAMPLE,
                to_jl_value_contents(pl.DataFrame()),
            )

    # If a file does not exist, one of the get_metadata/get_sample functions
    # will error.

    # Write the metadata to an Arrow file
    meta_path = get_meta_path(remotepath) if is_main else ""
    if curr_parameters_invalid:
        # Write `NamedTuple` with metadata to `meta_path` with `Arrow.write`
        Arrow.write(
            meta_path if is_main else IOBuffer(),
            path=remotepaths,
            nrows=meta_nrows,
            compress="zstd",
        )

    # Return LocationSource
    if is_main:
        # Construct the `Location` to return
        location_res = LocationSource(
            "Remote",
            {
                # For dispatching the appropriate PF for this format
                "format": format_string,
                # For constructing the `BanyanDataFrames.DataFrame`'s `nrows::Future` field
                "nrows": total_nrows,
                # For diagnostics purposes in PFs (partitioning functions)
                "path": remotepath,
                # For location constructor to use as caching
                "meta_path": meta_path,
                # For PFs to read from this source
                "empty_sample": empty_sample,
            },
            total_nbytes,
            remote_sample,
        )

        # Write out the updated `Location`
        cache_location(remotepath, location_res, invalidate_sample, invalidate_metadata)

        return location_res
    else:
        return NOTHING_LOCATION


def RemoteTableSource(
    remotepath,
    shuffled=True,
    metadata_invalid=False,
    sample_invalid=False,
    invalidate_metadata=False,
    invalidate_sample=False,
    max_exact_sample_length=None,
) -> Location:
    if max_exact_sample_length is None:
        max_exact_sample_length = get_max_exact_sample_length()

    loc = offloaded(
        _remote_table_source,
        remotepath,
        shuffled,
        metadata_invalid,
        sample_invalid,
        invalidate_metadata,
        invalidate_sample,
        max_exact_sample_length,
        distributed=True,
    )
    # TODO: Implement this
    loc.sample_value = DataFrames.DataFrame(Arrow.Table(seekstart(loc.sample.value)))
    return loc


# Load metadata for writing
# NOTE: `remotepath` should end with `.parquet` or `.csv` if Parquet
# or CSV dataset is desired to be created
def RemoteTableDestination(remotepath) -> Location:
    return LocationDestination(
        "Remote",
        {
            "format": get_file_ending(remotepath),
            "nrows": 0,
            "path": remotepath,
        },
    )
