from copy import deepcopy
import math
import numpy as np
import os
from pathlib import Path
import random
import shutil
from typing import Any, Callable, Dict, List

import numpy as np

from .clusters import get_cluster_s3_bucket_name
from .future import Future
from .futures import get_location
from .id import ValueId
from .location import LocationParameters, Location, LocationPath, get_sampling_config
from .request import RecordLocationRequest
from .requests import offloaded, record_request
from .sample import Sample
from .samples import ExactSample, NOTHING_SAMPLE
from .session import get_session, get_session_id
from .utils import (
    deserialize_retry,
    get_python_version,
    indexapply,
    serialize,
    total_memory_usage,
)
from .utils_pfs import to_py_value


################################
# Methods for setting location #
################################


def sourced(fut: Future, loc: Location):
    if loc.src_name is None:
        raise ("Location cannot be used as a source")

    if fut_location.is_none():
        located(
            fut,
            Location(
                loc.src_name,
                "None",
                loc.src_parameters,
                {},
                loc.sample_memory_usage,
                loc.sample if loc.sample.value is None else Sample(),
                loc.metadata_invalid,
                loc.sample_invalid,
            ),
        )
    else:
        fut_location: Location
        located(
            fut,
            Location(
                loc.src_name,
                fut_location.dest_name,
                loc.src_parameters,
                fut_location.dst_parameters,
                loc.sample_memory_usage,
                loc.sample if loc.sample.value is None else fut_location.sample,
                loc.metadata_invalid,
                loc.sample_invalid,
            ),
        )


def destined(fut: Future, loc: Location):
    if loc.dst_name is None:
        raise ("Location cannot be used as a destination")

    fut_location = get_location(fut)
    if fut_location is None:
        located(
            fut,
            Location(
                "None",
                loc.dst_name,
                {},
                loc.dst_parameters,
                fut_location.sample_memory_usage,
                Sample(),
                loc.metadata_invalid,
                loc.sample_invalid,
            ),
        )
    else:
        located(
            fut,
            Location(
                fut_location.src_name,
                loc.dst_name,
                fut_location.src_parameters,
                loc.dst_parameters,
                fut_location.sample_memory_usage,
                fut_location.sample,
                fut_location.metadata_invalid,
                fut_location.sample_invalid,
            ),
        )


# The purpose of making the source and destination assignment lazy is because
# location constructors perform sample collection and collecting samples is
# expensive. So after we write to a location and invalidate the cached sample,
# we only want to compute the new location source if the value is really used
# later on.

source_location_funcs: Dict[ValueId, Callable] = {}
destination_location_funcs: Dict[ValueId, Callable] = {}


def sourced(fut: Future, location_func: Any):
    global source_location_funcs
    source_location_funcs[fut.value_id] = location_func


def destined(fut: Future, location_func: Any):
    global destination_location_funcs
    destination_location_funcs[fut.value_id] = location_func


def apply_sourced_or_destined_funcs(fut: Future):
    global source_location_funcs
    global destination_location_funcs

    if fut.value_id in source_location_funcs:
        src_func = source_location_funcs[fut.value_id]
        new_source_location = src_func(fut)
        sourced(fut, new_source_location)
        source_location_funcs.pop(fut.value_id, None)

    if fut.value_id in destination_location_funcs:
        dst_func = destination_location_funcs[fut.value_id]
        new_destination_location = dst_func(fut)
        destined(fut, new_destination_location)
        destination_location_funcs.pop(fut.value_id, None)


def located(fut: Future, location: Location):
    session = get_session()
    value_id = fut.value_id

    # Store future's datatype in the parameters so that it could be used for
    # dispatching various PFs (partitioning functions).
    location.src_parameters["datatype"] = fut.datatype
    location.dst_parameters["datatype"] = fut.datatype

    if (location.src_name == "Client") or (location.dst_name == "Client"):
        session.futures_on_client[value_id] = fut
    else:
        # TODO: Set loc of all Futures with Client loc to None at end of
        # evaluate and ensure that this is proper way to handle Client
        session.futures_on_client.pop(value_id, None)

    session.locations[value_id] = location
    record_request(RecordLocationRequest(value_id, location))


################################
# Methods for getting location #
################################


def get_src_name(fut) -> str:
    return get_location(fut).src_name


def get_dst_name(fut) -> str:
    return get_location(fut).dst_name


def get_src_parameters(fut) -> LocationParameters:
    return get_location(fut).src_parameters


def get_dst_parameters(fut) -> LocationParameters:
    return get_location(fut).dst_parameters


####################
# Simple locations #
####################


def Value(val) -> Location:
    val_dict: Dict[str, Any] = {"value": to_py_value(val)}
    return LocationSource("Value", val_dict, sample_memory_usage(val), ExactSample(val))


# TODO: Implement Size
def Size(val) -> Location:
    val_dict: Dict[str, Any] = {"value": to_py_value(val)}
    return LocationSource(
        "Value", val_dict, 0, Sample(indexapply(getsamplenrows, val, 1), 1)
    )


def Client(val: Any) -> Location:
    val_dict: Dict[str, Any] = {}
    return LocationSource("Client", val_dict, sample_memory_usage(val), ExactSample(val))


CLIENT = Location(
    "None",
    "Client",
    LocationParameters(),
    LocationParameters(),
    0,
    Sample(None, 0, 1),
    False,
    False,
)


def Client() -> Location:
    return deepcopy(CLIENT)


# TODO: Un-comment only if Size is needed
# def Size(size):
#     return Value(size)
NONE_LOCATION = Location(
    "None",
    "None",
    LocationParameters(),
    LocationParameters(),
    0,
    Sample(None, 0, 1),
    False,
    False,
)


def Nothing() -> Location:  # TODO: Called None in jl
    return deepcopy(NONE_LOCATION)


# The scheduler intelligently determines when to split from and merge to disk even when no location is specified
DISK = NONE_LOCATION


def Disk() -> Location:
    return deepcopy(DISK)


# Values assigned "None" location as well as other locations may reassigned
# "Memory" or "Disk" locations by the scheduler depending on where the relevant
# data is.

# NOTE: Currently, we only support s3:// or http(s):// and only either a
# single file or a directory containing files that comprise the dataset.
# What we currently support:
# - Single HDF5 files (with .h5 or .hdf5 extension) with group at end of name
# - Single CSV/Parquet/Arrow files (with appropraite extensions)
# - Directories containing CSV/Parquet/Arrow files
# - s3:// or http(s):// (but directories and writing are not supported over the
# Internet)

# TODO: Add support for Client

# TODO: Implement Client, Remote for HDF5, Parquet, Arrow, and CSV so that they
# compute nrows ()

####################
# Remote locations #
####################

# NOTE: Sampling may be the source of weird and annoying bugs for users.
# Different values tracked by Banyan might have different sampling rates
# where one is the session's set sampling rate and the other has a sampling rate
# of 1. If it is expected for both the values to have the same size or be
# equivalent in some way, this won't be the case. The samples will have
# differerent size.
#
# Another edge case is when you have two dataframes each stored in S3 and they
# have the same number of rows and the order matters in a way that each row
# corresponds to the row at the same index in the other dataframe. We can get
# around this by using the same seed for every value we read in.
#
# Aside from these edge cases, we should be mostly okay though. We simply hold
# on to the first 1024 data points. And then swap stuff out randomly. We
# ensure that the resulting sample size is deterministaclly produced from the
# overall data size. This way, two arrays that have the same actual size will
# be guaranteed to have the same sample size.

# Things to think about when choosing max sample length
# - You might have massive 2D (or even higher dimensional) arrays
# - You might have lots of huge images
# - You might have lots of workers so your sample rate is really large

def getsamplenrows(totalnrows: int) -> int:
    sc = get_sampling_config()
    return math.ceil(totalnrows, 1 if sc.always_exact else sc.rate)


# We maintain a cache of locations and a cache of samples. Locations contain
# information about what files are in the dataset and how many rows they have
# while samples contain an actual sample from that dataset

# The invalidate_* and invalidate_all_* functions should be used if some actor that
# Banyan is not aware of mutates the location. Locations should be
# eventually stored and updated in S3 on each write.


def invalidate_metadata(p, **kwargs):
    lp = LocationPath(p, kwargs)

    # Delete locally
    p = os.path.join(os.path.expanduser("~"), ".banyan", "metadata", get_metadata_path(lp))
    if os.path.isfile(p):
        os.remove(p)

    # Delete from S3
    s3p = S3Path(f"s3://{banyan_metadata_bucket_name()}/{get_metadata_path(lp)}")
    if os.path.isfile(s3p):
        os.remove(s3p)

def invalidate_samples(p, **kwargs):
    lp = LocationPath(p, kwargs)

    # Delete locally
    sample_path_prefix = get_sample_path_prefix(lp)
    samples_local_dir = os.path.join(os.path.expanduser("~"), ".banyan", "samples", sample_path_prefix)
    if os.path.isdir(samples_local_dir)
        os.remove(samples_local_dir, recursive=true, force=true)
    # Delete from S3
    s3p = S3Path("s3://$(banyan_samples_bucket_name())/$sample_path_prefix")
    if !isempty(readdir_no_error(s3p))
        rm(path_as_dir(s3p), recursive=true)

function invalidate_location(p; kwargs...)
    invalidate_metadata(p; kwargs...)
    invalidate_samples(p; kwargs...)
end

function partition(series, partition_size)
    (series[i:min(i+(partition_size-1),end)] for i in 1:partition_size:length(series))
end
function invalidate_all_locations()
    for local_dir in [get_samples_local_path(), get_metadata_local_path()]
        rm(local_dir; force=true, recursive=true)
    end
    # Delete from S3
    for bucket_name in [banyan_samples_bucket_name(), banyan_metadata_bucket_name()]
        s3p = S3Path("s3://$bucket_name")
        if isdir_no_error(s3p)
            for p in readdir(s3p, join=true)
                rm(p, force=true, recursive=true)
            end
        end
    end 
end

function invalidate(p; after=false, kwargs...)
    if get(kwargs, after ? :invalidate_all_locations : :all_locations_invalid, false)
        invalidate_all_location()
    elseif get(kwargs, after ? :invalidate_location : :location_invalid, false)
        invalidate_location(p; kwargs...)
    else
        if get(kwargs, after ? :invalidate_metadata : :metadata_invalid, false)
            invalidate_metadata(p; kwargs...)
        end
        if get(kwargs, after ? :invalidate_samples : :samples_invalid, false)
            invalidate_samples(p; kwargs...)
        end
    end




class JL:
    pass



def get_sample_and_metadata(t: JL, p, sample_rate):
    data = deserialize_retry(p)
    return get_sample_from_data(data, sample_rate, size(data, 1)), size(data, 1)


function RemoteSource(
    lp::LocationPath,
    _remote_source::Function,
    load_sample::Function,
    load_sample_after_offloaded::Function,
    write_sample::Function,
    args...
)::Location
    # _remote_table_source(lp::LocationPath, loc::Location, sample_rate::Int64)::Location
    # load_sample accepts a file path
    # load_sample_after_offloaded accepts the sampled value returned by the offloaded function
    # (for BDF.jl, this is an Arrow blob of bytes that needs to be converted into an actual
    # dataframe once sent to the client side)

    # Look at local and S3 caches of metadata and samples to attempt to
    # construct a Location.
    loc, local_metadata_path, local_sample_path = get_location_source(lp)

    res = if !loc.metadata_invalid && !loc.sample_invalid
        # Case where both sample and parameters are valid
        loc.sample.value = load_sample(local_sample_path)
        loc.sample.rate = parse_sample_rate(local_sample_path)
        loc
    elseif loc.metadata_invalid && !loc.sample_invalid
        # Case where parameters are invalid
        new_loc = offloaded(_remote_source, lp, loc, args...; distributed=true, print_logs=false)
        Arrow.write(local_metadata_path, Arrow.Table(); metadata=new_loc.src_parameters)
        new_loc.sample.value = load_sample(local_sample_path)
        new_loc
    else
        # Case where sample is invalid

        # Get the Location with up-to-date metadata (source parameters) and sample
        new_loc = offloaded(_remote_source, lp, loc, args...; distributed=true, print_logs=false)
        # @show new_loc

        if !loc.metadata_invalid
            # Store the metadata locally. The local copy just has the source
            # parameters but PFs can still access the S3 copy which will have the
            # table of file names and #s of rows.
            Arrow.write(local_metadata_path, Arrow.Table(); metadata=new_loc.src_parameters)
        end

        # Store the Arrow sample locally and update the returned Sample
        write_sample(local_sample_path, new_loc.sample.value)
        new_loc.sample.value = load_sample_after_offloaded(new_loc.sample.value)

        new_loc
    end
    res
