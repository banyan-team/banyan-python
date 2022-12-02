from copy import deepcopy
from datetime import datetime
import logging
import os
import shutil
import traceback
from typing import Any, Dict, Optional, Tuple, Union

from botocore.exceptions import ClientError
import boto3
from plum import dispatch
import pyarrow as pa
from s3path import S3Path

from .sample import Sample, SamplingConfig
from .samples import NOTHING_SAMPLE
from .sessions import _get_session_id_no_error
from .utils import (
    get_organization_id,
    get_python_version,
    is_debug_on,
    parse_bytes,
    readdir_no_error,
)

logger = logging.getLogger("__name__")
s3 = boto3.client("s3")


LOCATION_PATH_KWARG_NAMES = ["add_channelview"]
TABLE_FORMATS = ["csv", "parquet", "arrow"]

LocationParameters = Dict[str, Any]

class Location:
    @dispatch
    def __init__(
        self,
        src_name: str,
        dst_name: str,
        src_parameters: LocationParameters,
        dst_parameters: LocationParameters,
        sample_memory_usage: int,
        sample: Sample,
        metadata_invalid: bool,
        sample_invalid: bool,
    ):

        self.src_name = src_name
        self.dst_name = dst_name
        self.src_parameters = src_parameters
        self.dst_parameters = dst_parameters
        self.total_memory_usage = sample_memory_usage
        self.sample = sample
        self.parameters_invalid = metadata_invalid
        self.sample_invalid = sample_invalid

    def is_none(self):
        return self.sample.is_none()

    def to_py(self):
        return {
            "src_name": self.src_name,
            "dst_name": self.dst_name,
            "src_parameters": self.src_parameters,
            "dst_parameters": self.dst_parameters,
            # NOTE: sample.properties[:rate] is always set in the Sample
            # constructor to the configured sample rate (default 1/nworkers) for
            # this session
            # TODO: Instead of computing the total memory usage here, compute it
            # at the end of each `@partitioned`. That way we will count twice for
            # mutation
            "sample_memory_usage": (
                None if (self.sample_memory_usage == -1) else self.sample_memory_usage,
            ),
        }


class LocationPath:
    @dispatch
    def __init__(self, path: str, format_name: str, format_version: str, **kwargs):
        # This function is responsible for "normalizing" the path.
        # If there are multiple path strings that are technically equivalent,
        # this function should map them to the same string.

        # Add the kwargs to the path
        path_res = deepcopy(path)
        for (kwarg_name, kwarg_value) in kwargs.items():
            if kwarg_name in LOCATION_PATH_KWARG_NAMES:
                path_res += f"_{kwarg_name}={kwarg_value}"

        # Return the LocationPath
        path_hash = hash(path_res)
        self.original_path = path_res
        self.path = path_res
        self.path_hash_uint = path_hash
        self.path_hash = str(path_hash)
        self.format_name = format_name
        self.format_version = format_version

    @dispatch
    def __init__(self, p: Any, format_name: str, format_version: str, **kwargs):
        self.__init__(f"lang_py_{hash(p)}", format_name, format_version, kwargs)

    @dispatch
    def __init__(self, p: str, **kwargs):
        if (p is None) or (p == ""):
            self.__init__("", "", "")  # NO_LOCATION_PATH
            return

        format_name = kwargs.get("format", "py")
        is_sample_format_arrow = format_name == "arrow"
        if is_sample_format_arrow:
            self.__init__(p, "arrow", kwargs.get("format_version", "2"), kwargs)
        else:
            for table_format in TABLE_FORMATS:
                if (table_format in p) or (format_name == p):
                    self.__init__(p, "arrow", "2", kwargs)
                    return
        self.__init__(p, "py", get_python_version(), kwargs)

    @dispatch
    def __init__(self, p: Any, **kwargs):
        self.__init__(f"lang_py_{hash(p)}", kwargs)

    def __hash__(self):
        return self.path_hash_uint


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
    parameters: Union[Dict[str,Any],Dict[str, str]],
    sample_memory_usage: int = -1,
    sample: Sample = Sample(),
):
    return Location(
        name,
        "None",
        parameters,
        LocationParameters(),
        sample_memory_usage,
        sample,
        False,
        False,
    )


def LocationDestination(name: str, parameters: Union[Dict[str,Any],Dict[str, str]]):
    return Location(
        "None", name, LocationParameters(), parameters, -1, Sample(), False, False
    )


# Functions with `LocationPath`s`


def get_sample_path_prefix(lp: LocationPath):
    format_name_sep = (
        "_" if ((lp.format_name is not None) and (lp.format_name != "")) else ""
    )
    return lp.path_hash + "_" + lp.format_name + format_name_sep + lp.format_version


def get_metadata_path(lp: LocationPath):
    return lp.path_hash


def banyan_samples_bucket_name():
    return f"banyan-samples-{get_organization_id()}"


def banyan_metadata_bucket_name():
    return f"banyan-metadata-{get_organization_id()}"


NO_LOCATION_PATH = LocationPath("", "", "")

# Sample config management

DEFAULT_SAMPLING_CONFIG = SamplingConfig(1024, False, parse_bytes("32 MB"), False, True)

session_sampling_configs = {"": {NO_LOCATION_PATH: DEFAULT_SAMPLING_CONFIG}}


def set_sampling_configs(d: Dict[LocationPath, SamplingConfig]):
    global session_sampling_configs
    session_sampling_configs[_get_session_id_no_error()] = d


def get_sampling_config(path="", **kwargs):
    return get_sampling_config(LocationPath(path, kwargs))


def get_sampling_configs():
    global session_sampling_configs
    return session_sampling_configs[_get_session_id_no_error()]


def get_sampling_config(l_path: LocationPath) -> SamplingConfig:
    scs = get_sampling_configs()
    return scs.get(l_path, scs[NO_LOCATION_PATH])


# Getting sample rate


def get_sample_rate(p="", **kwargs):
    return get_sample_rate(LocationPath(p, kwargs))


def parse_sample_rate(object_key):
    int(os.path.split(object_key)[-1])


def get_sample_rate(l_path: LocationPath):
    sc = get_sampling_config(l_path)

    # Get the desired sample rate
    desired_sample_rate = sc.rate

    # If we just want the default sample rate or if a new sample rate is being
    # forced, then just return that.
    if (l_path.path is None) or (l_path.path == ""):
        return desired_sample_rate
    if sc.force_new_sample_rate:
        return desired_sample_rate

    # Find a cached sample with a similar sample rate
    banyan_samples_bucket = S3Path(f"s3://{banyan_samples_bucket_name()}")
    banyan_samples_object_dir = os.path.join(
        banyan_samples_bucket, get_sample_path_prefix(l_path)
    )
    sample_rate = -1
    for object_key in readdir_no_error(banyan_samples_object_dir):
        object_sample_rate = int(object_key)
        object_sample_rate_diff = abs(object_sample_rate - desired_sample_rate)
        curr_sample_rate_diff = abs(sample_rate - desired_sample_rate)
        if (sample_rate == -1) or (object_sample_rate_diff < curr_sample_rate_diff):
            sample_rate = object_sample_rate
    if sample_rate != -1:
        return sample_rate
    else:
        return desired_sample_rate


# Checking for having metadata, samples


def has_metadata(l_path: Union[str, LocationPath], **kwargs):
    if isinstance(l_path, str):
        l_path = LocationPath(l_path, kwargs)
    return S3Path(
        f"s3://{banyan_metadata_bucket_name()}/{get_metadata_path(l_path)}"
    ).isfile()


def has_sample(l_path: LocationPath, **kwargs) -> bool:
    if isinstance(l_path, str):
        l_path = LocationPath(l_path, kwargs)
    sc = get_sampling_config(l_path)
    banyan_sample_dir = S3Path(
        "s3://$(banyan_samples_bucket_name())/$(get_sample_path_prefix(l_path))"
    )
    if sc.force_new_sample_rate:
        return os.path.isfile(os.path.join(banyan_sample_dir, str(sc.rate)))
    else:
        return len(readdir_no_error(banyan_sample_dir)) != 0


# Helper function for getting `Location` for location constructors


def twodigit(i: int):
    if i < 10:
        return "0" + str(i)
    else:
        return str(i)


def get_src_params_dict(d: Dict[str, str]):
    return {} if (d is None) else d


def PyArrowTable(p) -> pa.Table:
    with pa.memory_map(p, "rb") as source:
        array = pa.ipc.open_file(source).read_all()
        return pa.Table.from_arrays(array)


def get_src_params_dict_from_arrow(p):
    return get_src_params_dict(PyArrowTable(p).schema.metadata)


def get_metadata_local_path():
    p = os.path.join(os.path.expanduser("~"), ".banyan", "metadata")
    if not os.path.isdir(p):
        os.makedirs(p)
    return p

def get_samples_local_path():
    p = os.path.join(os.path.expanduser("~"), ".banyan", "samples")
    if not os.path.isdir(p):
        os.makedirs(p)
    return p

def get_location_source(lp: LocationPath) -> Tuple[Location, str, str]:
    global s3

    # This checks local cache and S3 cache for sample and metadata files.
    # It then returns a Location object (with a null sample) and the local file names
    # to read/write the metadata and sample from/to.

    # Load in metadata
    metadata_path = get_metadata_path(lp)
    metadata_local_path = os.path.join(get_metadata_local_path(), metadata_path)
    metadata_bucket_name = banyan_metadata_bucket_name()
    metadata_s3_path = f"/{banyan_metadata_bucket_name()}/{metadata_path}"
    src_params_not_stored_locally = False
    if os.path.isfile(metadata_local_path):
        lm = datetime.fromtimestamp(os.path.getmtime(metadata_local_path))
        if_modified_since_string = (
            f"${lm.strftime('%A')}, {twodigit(lm.day)} {lm.strftime('%b')} {lm.year} "
            "{twodigit(lm.hour)}:{twodigit(lm.minute)}:{twodigit(lm.second)} GMT"
        )
        try:
            d = get_src_params_dict_from_arrow(s3.get_object(Bucket=metadata_bucket_name, Key=metadata_path, IfModifiedSince=lm)["Body"].read())
            src_params_not_stored_locally = True
            src_params = d
        except ClientError as e:
            # error_code = e.response["Error"]["Code"]
            error_status_code = e.response["ResponseMetadata"]["HTTPStatusCode"]
            if is_debug_on():
                traceback.print_exc()
            if error_status_code == 404:
                src_params = {}
            elif error_status_code == 304:
                src_params = get_src_params_dict_from_arrow(metadata_local_path)
            else:
                logger.warn("Assumming locally stored metadata is invalid because of following error in accessing the metadata copy in the cloud")
                traceback.print_exc()
                src_params = {}
    else:
        try:
            d = get_src_params_dict_from_arrow(s3.get_object(Bucket=metadata_bucket_name, Key=metadata_path)["Body"].read())
            src_params_not_stored_locally = True
            src_params = d
        except ClientError as e:
            error_status_code = e.response["ResponseMetadata"]["HTTPStatusCode"]
            if is_debug_on():
                traceback.print_exc()
            if error_status_code == 404:
                logger.warn("Assuming metadata isn't copied in the cloud because of following error in attempted access")
                traceback.print_exc()
            src_params = {}
    # Store metadata locally
    if src_params_not_stored_locally and (len(src_params) != 0):
        Arrow.write(metadata_local_path, Arrow.Table(); metadata=src_params)

    # Load in sample

    sc = get_sampling_config(lp)
    force_new_sample_rate = sc.force_new_sample_rate
    desired_sample_rate = sc.rate
    sample_path_prefix = get_sample_path_prefix(lp)

    # Find local samples
    found_local_samples = []
    found_local_sample_rate_diffs = []
    sample_local_dir = os.path.join(get_samples_local_path(), sample_path_prefix)
    os.makedirs(sample_local_dir)
    local_sample_paths = os.listdir(sample_local_dir) if os.path.isdir(sample_local_dir) else []
    for local_sample_path_suffix in local_sample_paths:
        local_sample_path = os.path.join(sample_local_dir, local_sample_path_suffix)
        local_sample_rate = int(local_sample_path_suffix)
        diff_sample_rate = abs(local_sample_rate - desired_sample_rate)
        if (not force_new_sample_rate) or (diff_sample_rate == 0):
            found_local_samples.append((local_sample_path, local_sample_rate))
            found_local_sample_rate_diffs.append(diff_sample_rate)

    # Sort in descending suitability (the most suitable sample is the one with sample
    # rate closest to the desired sample rate)
    found_local_samples = found_local_samples[sortperm(found_local_sample_rate_diffs)]

    # Find a local sample that is up-to-date. NOTE: The data itself might have
    # changed in which case the cached samples are out-of-date and we don't
    # currently capture that. This doesn't even check if there is a more recent
    # sample of a different sample rate (although that is kind of a bug/limitation
    # that could be resolved though the best way to resolve it would be by
    # comparing to the last modified date for the data itself). It just checks that the remote sample
    # hasn't been manually invalidated by the user or a Banyan writing function
    # and that there isn't a newer sample for this specific sample rate.
    final_local_sample_path = ""
    final_sample_rate = -1
    for (sample_local_path, sample_rate) in found_local_samples:
        lm = datetime.fromtimestamp(os.path.getmtime(sample_local_path))
        sample_s3_bucket_name = banyan_samples_bucket_name()
        sample_s3_path = f"{sample_path_prefix}/{sample_rate}"
        try:
            blob = s3.get_object(Bucket=sample_s3_bucket_name, Key=sample_s3_path, IfModifiedSince=lm)["Body"].read()
            write(sample_local_path, seekstart(blob.io))  # This overwrites the existing file
            final_local_sample_path = sample_local_path
            final_sample_rate = sample_rate
            break
        except ClientError as e:
            # error_code = e.response["Error"]["Code"]
            error_status_code = e.response["ResponseMetadata"]["HTTPStatusCode"]
            if is_debug_on():
                traceback.print_exc()
            if error_status_code == 404:
                logger.warn("Assumming locally stored metadata is invalid because it is not backed up to the cloud")
            elif error_status_code == 304:
                final_local_sample_path = sample_local_path
                final_sample_rate = sample_rate
                break
            else:
                logger.warn("Assumming locally stored metadata is invalid because of following error in accessing the metadata copy in the cloud")
                traceback.print_exc()

    # If no such sample is found, search the S3 bucket
    banyan_samples_bucket = S3Path(f"s3://{banyan_samples_bucket_name()}")
    banyan_samples_object_dir = os.path.join(banyan_samples_bucket, sample_path_prefix)
    if (final_local_sample_path is not None) and (final_local_sample_path != ""):
        final_sample_rate = -1
        for object_key in readdir_no_error(banyan_samples_object_dir):
            object_sample_rate = int(object_key)
            object_sample_rate_diff = abs(object_sample_rate - desired_sample_rate)
            curr_sample_rate_diff = abs(final_sample_rate - desired_sample_rate)
            if (
                (object_sample_rate_diff == 0)
                if force_new_sample_rate
                else (
                    (final_sample_rate == -1) or (object_sample_rate_diff < curr_sample_rate_diff)
                )
            ):
                final_sample_rate = object_sample_rate
                final_local_sample_path = os.path.join(sample_local_dir, object_key)
        if final_sample_rate != -1:
            shutil.copyfile(
                os.path.join(
                    banyan_samples_bucket,
                    sample_path_prefix,
                    str(final_sample_rate)
                ),
                final_local_sample_path
            )

    # Construct and return LocationSource
    res_location = LocationSource(
        src_params.get("name", "Remote"),
        src_params,
        int(src_params.get("sample_memory_usage", "0")),
        deepcopy(NOTHING_SAMPLE)
    )
    res_location.metadata_invalid = (src_params is None) or (src_params == "")
    res_location.sample_invalid = (final_local_sample_path is None) or (final_local_sample_path == "")
    final_sample_rate = desired_sample_rate if ((final_local_sample_path is None) or (final_local_sample_path == "")) else final_sample_rate
    return res_location, metadata_local_path, os.path.join(sample_local_dir, str(final_sample_rate))
