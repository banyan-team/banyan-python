from copy import deepcopy
import math
import os
from pathlib import Path
import random
import shutil
from typing import Any, Callable, Dict, List

from clusters import get_cluster_s3_bucket_name
from future import Future
from futures import get_location
from id import ValueId
from location import LocationParameters, Location
from request import RecordLocationRequest
from requests import offloaded, record_request
from sample import Sample
from samples import ExactSample, NOTHING_SAMPLE
from session import get_session, get_session_id
from utils import (
    deserialize_retry,
    get_python_version,
    indexapply,
    serialize,
    total_memory_usage,
)
from utils_pfs import to_py_value

#################
# Location type #
#################

NOTHING_LOCATION = Location(
    "None",
    "None",
    LocationParameters(),
    LocationParameters(),
    int(-1),
    NOTHING_SAMPLE,
    False,
    False,
)

INVALID_LOCATION = Location(
    "None",
    "None",
    LocationParameters(),
    LocationParameters(),
    int(-1),
    NOTHING_SAMPLE,
    True,
    True,
)


def LocationSource(
    name: str,
    parameters: LocationParameters,
    total_memory_usage: int = -1,
    sample: Sample = Sample(),
):
    return Location(
        name,
        "None",
        parameters,
        LocationParameters(),
        total_memory_usage,
        sample,
        False,
        False,
    )


def LocationDestination(name: str, parameters: LocationParameters):
    return Location(
        "None", name, LocationParameters(), parameters, -1, Sample(), False, False
    )


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
                loc.total_memory_usage,
                loc.sample if loc.sample.value is None else Sample(),
                loc.parameters_invalid,
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
                loc.total_memory_usage,
                loc.sample if loc.sample.value is None else fut_location.sample,
                loc.parameters_invalid,
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
                fut_location.total_memory_usage,
                Sample(),
                loc.parameters_invalid,
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
                fut_location.total_memory_usage,
                fut_location.sample,
                fut_location.parameters_invalid,
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
    return LocationSource("Value", val_dict, total_memory_usage(val), ExactSample(val))


# TODO: Implement Size
def Size(val) -> Location:
    val_dict: Dict[str, Any] = {"value": to_py_value(val)}
    return LocationSource(
        "Value", val_dict, 0, Sample(indexapply(getsamplenrows, val, 1))
    )


def Client(val: Any) -> Location:
    val_dict: Dict[str, Any] = {}
    return LocationSource("Client", val_dict, total_memory_usage(val), ExactSample(val))


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

MAX_EXACT_SAMPLE_LENGTH = int(os.getenv("BANYAN_MAX_EXACT_SAMPLE_LENGTH", "1024"))


def get_max_exact_sample_length() -> int:
    return MAX_EXACT_SAMPLE_LENGTH


def set_max_exact_sample_length(val):
    global MAX_EXACT_SAMPLE_LENGTH
    MAX_EXACT_SAMPLE_LENGTH = val


def getsamplenrows(totalnrows: int) -> int:
    if totalnrows <= get_max_exact_sample_length():
        # NOTE: This includes the case where the dataset is empty
        # (totalnrows == 0)
        return totalnrows
    else:
        # Must have at least 1 row
        return math.ceil(totalnrows, get_session().sample_rate)


# We maintain a cache of locations and a cache of samples. Locations contain
# information about what files are in the dataset and how many rows they have
# while samples contain an actual sample from that dataset

# The invalidate_* and invalidate_all_* functions should be used if some actor that
# Banyan is not aware of mutates the location. Locations should be
# eventually stored and updated in S3 on each write.


def _invalidate_all_locations():
    for dir_name in ["banyan_locations", "banyan_meta"]:
        shutil.rmtree(
            "s3/$(get_cluster_s3_bucket_name())/$dir_name/", ignore_errors=True
        )  # TODO: OK?
        # rm("s3/$(get_cluster_s3_bucket_name())/$dir_name/", force=true, recursive=true)


def _invalidate_metadata(remotepath):
    p = get_location_path(remotepath)
    if os.path.isfile(p):
        loc = deserialize_retry(p)
        loc.parameters_invalid = True
        serialize(p, loc)


def _invalidate_sample(remotepath):
    p = get_location_path(remotepath)
    if os.path.isfile(p):
        loc = deserialize_retry(p)
        loc.sample_invalid = True
        serialize(p, loc)


def invalidate_all_location():
    return offloaded(_invalidate_all_locations)


def invalidate_metadata(p):
    return offloaded(_invalidate_metadata, p)


def invalidate_sample(p):
    return offloaded(_invalidate_sample, p)


# @specialize TODO: Not sure what to do with this

# Helper functions for location constructors; these should only be called from the main worker

# TODO: Hash in a more general way so equivalent paths hash to same value
# This hashes such that an extra slash at the end won't make a difference``
def get_remotepath_id(remotepath: str):
    return (get_python_version(), (hash(os.path.join(Path(remotepath).parts))))


def get_location_path(remotepath, remotepath_id):
    session_s3_bucket_name = get_cluster_s3_bucket_name()
    if not os.path.isdir(f"s3/{session_s3_bucket_name}/banyan_locations/"):
        os.path.mkdir(f"s3/{session_s3_bucket_name}/banyan_locations/")

    return f"s3/$session_s3_bucket_name/banyan_locations/$(remotepath_id)"


def get_meta_path(remotepath, remotepath_id):
    session_s3_bucket_name = get_cluster_s3_bucket_name()
    if not os.path.isdir(f"s3/{session_s3_bucket_name}/banyan_meta/"):
        os.path.mkdir(f"s3/{session_s3_bucket_name}/banyan_meta/")

    return f"s3/{session_s3_bucket_name}/banyan_meta/{remotepath_id}"


def get_location_path(remotepath):
    return get_location_path(remotepath, get_remotepath_id(remotepath))


def get_meta_path(remotepath):
    return get_meta_path(remotepath, get_remotepath_id(remotepath))


def get_cached_location(remotepath, remotepath_id, metadata_invalid, sample_invalid):
    random.seed(hash((get_session_id(), remotepath_id)))
    session_s3_bucket_name = get_cluster_s3_bucket_name()
    location_path = "s3/$session_s3_bucket_name/banyan_locations/$remotepath_id"

    try:
        curr_location: Location = deserialize_retry(location_path)
    except:
        curr_location: Location = INVALID_LOCATION

    curr_location.sample_invalid = curr_location.sample_invalid or sample_invalid
    curr_location.parameters_invalid = (
        curr_location.parameters_invalid or metadata_invalid
    )
    curr_sample_invalid = curr_location.sample_invalid
    curr_parameters_invalid = curr_location.parameters_invalid
    curr_location, curr_sample_invalid, curr_parameters_invalid


def get_cached_location(remotepath, metadata_invalid, sample_invalid):
    return get_cached_location(
        remotepath, get_remotepath_id(remotepath), metadata_invalid, sample_invalid
    )


def cache_location(
    remotepath,
    remotepath_id,
    location_res: Location,
    invalidate_sample,
    invalidate_metadata,
):
    location_path = get_location_path(remotepath, remotepath_id)
    location_to_write = deepcopy(location_res)
    location_to_write.sample_invalid = (
        location_to_write.sample_invalid or invalidate_sample
    )
    location_to_write.parameters_invalid = (
        location_to_write.parameters_invalid or invalidate_metadata
    )
    serialize(location_path, location_to_write)


def cache_location(
    remotepath, location_res: Location, invalidate_sample, invalidate_metadata
):
    return cache_location(
        remotepath,
        get_remotepath_id(remotepath),
        location_res,
        invalidate_sample,
        invalidate_metadata,
    )


# Functions to be extended for different data formats


def sample_from_range(r, sample_rate):
    # TODO: Maybe return whole range if sample rate is 1
    len = len(r)
    sample_len = int(math.ceil(len / sample_rate))
    rand_indices = randsubseq(
        range(len), 1 / sample_rate
    )  # TODO: Not sure how to do this
    if len(rand_indices) > sample_len:
        rand_indices = rand_indices[1:sample_len]
    else:
        while len(rand_indices) < sample_len:
            new_index = random.randrange(1, sample_len)
            if new_index not in rand_indices:
                rand_indices.append(new_index)

    return rand_indices


class JL:
    pass


def has_separate_metadata(t: JL):
    return False


def get_metadata(t: JL, p):
    return size(deserialize_retry(p), 1)


def get_sample_from_data(data, sample_rate, len: int):
    return get_sample_from_data(
        data, sample_rate, sample_from_range(range(len), sample_rate)
    )


def get_sample_from_data(data, sample_rate, rand_indices: List[int]):
    if sample_rate == 1.0:
        return data

    data_ndims = ndims(data)  # TODO???
    # TODO: next line?
    data_selector = Base.fill(Colon(), data_ndims)
    data_selector[0] = rand_indices
    return data[data_selector:]


def get_sample(t: JL, p, sample_rate, len):
    data = deserialize_retry(p)
    return get_sample_from_data(data, sample_rate, len)


def get_sample_and_metadata(t: JL, p, sample_rate):
    data = deserialize_retry(p)
    return get_sample_from_data(data, sample_rate, size(data, 1)), size(data, 1)
