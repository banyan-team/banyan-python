from copy import deepcopy
from typing import Any, Dict, List, Tuple

from plum import dispatch

from .future import Future
from .location import session_sampling_configs
from .locations import get_location
from .sample import Sample, SamplingConfig
from .utils import total_memory_usage


def configure_sampling(
    path="",
    nworkers=None,
    sample_rate=None,
    always_exact=None,
    max_num_bytes_exact=None,
    force_new_sample_rate=None,
    assume_shuffled=None,
    for_all_locations=False,
    default=False,
    **kwargs,
):
    global session_sampling_configs

    if sample_rate is None:
        sample_rate = (nworkers * 8) if (nworkers is not None) else sc.rate
    if always_exact is None:
        always_exact = sc.always_exact
    if max_num_bytes_exact is None:
        max_num_bytes_exact = sc.max_num_bytes_exact
    if force_new_sample_rate is None:
        force_new_sample_rate = sc.force_new_sample_rate
    if assume_shuffled is None:
        assume_shuffled = sc.assume_shuffled

    sc = DEFAULT_SAMPLING_CONFIG if default else get_sampling_config(path, kwargs)
    nsc = SamplingConfig(
        sample_rate,
        always_exact,
        max_num_bytes_exact,
        force_new_sample_rate,
        assume_shuffled,
    )

    session_id = _get_session_id_no_error()
    lp = LocationPath(path, kwargs)
    sampling_configs = session_sampling_configs[session_id]
    if for_all_locations:
        sampling_configs = {}
        sampling_configs[NO_LOCATION_PATH] = nsc
    else:
        sampling_configs[lp] = nsc


###############################################################
# Sample that caches properties returned by an AbstractSample #
###############################################################

# Implementation error
def impl_error(fn_name, type_as):
    raise (f"{fn_name} not implemented for {type(type_as)}")


# Functions to implement for Any (e.g., for DataFrame or Array


def sample_by_key(type_as: Any, key: Any):
    impl_error("sample_by_key", type_as)


def sample_axes(type_as: Any) -> List[int]:
    impl_error("sample_axes", type_as)


def sample_keys(type_as: Any):
    impl_error("sample_keys", type_as)


@dispatch
def ExactSample(value: Any):
    return Sample(value, 1)


@dispatch
def ExactSample(value: Any, memory_usage: int):
    return Sample(value, memory_usage, 1)


def setsample(fut: Future, value: Any):
    s: Sample = get_location(fut).sample
    memory_usage: int = total_memory_usage(value)
    rate: int = s.rate
    s.value = value
    s.memory_usage = memory_usage
    s.objectid = id(value)


# sample* functions always return a concrete value or a dict with properties.
# To get a `Sample`, access the property.

# TODO: Lazily compute samples by storing sample computation in a DAG if its
# getting too expensive
def sample(fut: Future):
    return get_location(fut).sample.value


class SampleComputationCache:
    def __init__(self, computation: Dict[int, Any], same_keys: Dict[int, List[int]]):
        self.computation = computation
        self.same_keys = same_keys


sample_computation_cache = SampleComputationCache({}, {})


def get_sample_computation_cache() -> SampleComputationCache:
    global sample_computation_cache
    return sample_computation_cache


def insert_in_sample_computation_cache(
    cache: SampleComputationCache, key: int, other_key: int
):
    if key not in cache.same_keys:
        cache.same_keys[key] = [key, other_key]
    else:
        cache.same_keys[key].append(other_key)


def get_key_for_sample_computation_cache(
    cache: SampleComputationCache, key: int
) -> int:
    if key not in cache.same_keys:
        cache.same_keys[key] = [key]
        return 0

    for other_key in cache.same_keys[key]:
        if other_key in cache.computation:
            return other_key
    return 0


def keep_same_statistics(a: Future, a_key: Any, b: Future, b_key: Any):
    cache = get_sample_computation_cache()
    # Note that this runs after all samples have been computed so the objectid's
    # of the values should be right.
    a_objectid: int = get_location(a).sample.objectid
    b_objectid: int = get_location(b).sample.objectid
    for computation_func in ["sample_divisions", "orderinghashes"]:
        a_cache_key = hash((computation_func, a_objectid, a_key))
        b_cache_key = hash((computation_func, b_objectid, b_key))
        insert_in_sample_computation_cache(cache, a_cache_key, b_cache_key)
        insert_in_sample_computation_cache(cache, b_cache_key, a_cache_key)


# Sample computation functions


def orderinghashes(df: Any, key: Any):
    cache = get_sample_computation_cache()
    cache_key = hash(("orderinghashes", id(df), key))
    in_cache = get_key_for_sample_computation_cache(cache, cache_key)
    if in_cache != 0:
        return cache.computation[in_cache]

    data = sample_by_key(df, key)
    res = list(map(orderinghash, data))
    cache.computation[cache_key] = res
    return res


def get_all_divisions(data: List, ngroups: int) -> List:
    datalength = len(data)
    grouplength = datalength // ngroups
    # We use `unique` here because if the divisions have duplicates, this could
    # result in different partitions getting the same divisions. The usage of
    # `unique` here is more of a safety precaution. The # of divisions we use
    # is the maximum # of groups.
    # TODO: Ensure that `unique` doesn't change the order
    all_divisions: List = []
    used_index_pairs: List[Tuple[int, int]] = []
    for i in range(1, ngroups + 1):
        startindex = (i - 1) * grouplength + 1
        endindex = datalength if i == ngroups else i * grouplength + 1
        index_pair = (startindex, endindex)
        if index_pair not in used_index_pairs:
            all_divisions.append(
                # Each group has elements that are >= start and < end
                (data[startindex], data[endindex])
            )
            used_index_pairs.append(index_pair)
    return all_divisions


def sample_divisions(df: Any, key: Any):
    cache = get_sample_computation_cache()
    cache_key = hash(("sample_divisions", id(df), key))
    in_cache = get_key_for_sample_computation_cache(cache, cache_key)
    if in_cache != 0:
        return cache.computation[in_cache]

    # There are no divisions for empty data
    if len(df) == 0:
        return []

    max_ngroups = sample_max_ngroups(df, key)
    ngroups = min(max_ngroups, 512)
    data_unsorted = orderinghashes(df, key)
    data = deepcopy(data_unsorted)
    data.sort(reverse=False)
    all_divisions = get_all_divisions(data, ngroups)
    cache.computation[cache_key] = all_divisions
    return all_divisions


def sample_percentile(df: Any, key: Any, minvalue: float, maxvalue: float) -> float:
    # If the data frame is empty, nothing between `minvalue` and `maxvalue` can
    # exist in `df`. so the percentile is 0.
    if len(df) == 0 or minvalue is None or maxvalue is None:
        return 0.0

    # NOTE: This may cause some problems because the way that data is ultimately split may
    # not allow a really fine division of groups. So in the case of some filtering, the rate
    # of filtering may be 90% but if there are only like 3 groups then maybe it ends up being like
    # 50% and that doesn't get scheduled properly. We can try to catch stuff like maybe by using
    # only 70% of total memory in scheduling or more pointedly by changing this function to
    # call sample_divisions with a reasonable number of divisions and then counting how many
    # divisions the range actually belongs to.

    c = 0
    num_rows = 0
    ohs = orderinghashes(df, key)
    for oh in ohs:
        if (minvalue <= oh) and (oh <= maxvalue):
            c += 1
        num_rows += 1
    return c / num_rows


def sample_max_ngroups(type_as: Any, key: Any) -> int:
    if len(type_as) == 0:
        return 0
    else:
        data = sample_by_key(type_as, key)
        data_counter = counter(data)
        max_nrow_group = maximum(values(data_counter))
        return len(data) // max_nrow_group


def _minimum(ohs: List[Any]) -> Any:
    oh_min = ohs[1]
    for oh in ohs:
        oh_min = oh if oh <= oh_min else oh_min
    return oh_min


def _maximum(ohs: List[Any]) -> Any:
    oh_max = ohs[1]
    for oh in ohs:
        oh_max = oh if oh_max <= oh else oh_max
    return oh_max


# TODO: Maybe instead just do `orderinghash(minimum(sample_by_key(A, key)))``


def sample_min(A: Any, key: Any):
    return None if len(A) == 0 else _minimum(orderinghashes(A, key))


def sample_max(A: Any, key: Any):
    return None if len(A) == 0 else _maximum(orderinghashes(A, key))


# TODO: Not sure what to do with below line
# Base.isnothing(s::Sample) = s.rate == -1

# Caching samples with same statistics

# A sample with memoized statistics for
# Must be mutable so that the Future finalizer runs
class SampleForGrouping:
    def __init__(self, future: Future, sample: Any, keys: List[Any], axes: List[int]):
        self.future = future
        self.sample = sample
        self.keys = keys
        self.axes = axes


# Note that filtered_to's sample might be a vector

# This functions is for retrieving a sample of a future with same
# statistics properties with this key. Note that this function is not
# type-stable and its return type isn't knowable so it _will_ result
# in type inference. The best way to deal with that is to make sure to pass
# the result of calling this (or even the other `sample` functions) into a
# separate function to actually process its statistics. This creates a function
# barrier and type inference will still happen at run time but it will only
# happen once.
@dispatch
def sample_for_grouping(f: Future, keys: List[Any], f_sample: Any) -> SampleForGrouping:
    return SampleForGrouping(f, f_sample, keys, sample_axes(f_sample))


@dispatch
def sample_for_grouping(f: Future, keys: List[Any]):
    return sample_for_grouping(f, keys, sample(f))


@dispatch
def sample_for_grouping(f: Future, key: Any):
    return sample_for_grouping(f, [key])


# TODO: Not sure what to do for below line
# function sample_for_grouping(f::Future, ::Type{K}) where {K} sample_for_grouping(f, convert(Vector{K}, get_location(f).sample.groupingkeys)::Vector{K}) end
@dispatch
def sample_for_grouping(f: Future):
    return sample_for_grouping(f, int)
