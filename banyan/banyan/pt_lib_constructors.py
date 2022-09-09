from copy import deepcopy
import math
from typing import Callable, Dict, List, Union

from plum import dispatch

from .future import Future
from .futures import get_location
from .partitions import AtMost, Scale
from .sample import Sample
from .samples import (
    sample,
    sample_axes,
    sample_divisions,
    SampleForGrouping,
    sample_max,
    sample_max_ngroups,
    sample_min,
    sample_percentile,
)
from .utils import to_py_value
from .utils_partitions import PartitionType


def noscale(f):
    return Scale(f, by=1.0)


REPLICATING = PartitionType(("name", "Replicating"), noscale)


def Replicating() -> PartitionType:
    return deepcopy(REPLICATING)


REPLICATED = PartitionType(("name", "Replicating"), noscale, ("replication", "all"))


def Replicated() -> PartitionType:
    return deepcopy(REPLICATED)


DIVIDED = PartitionType(("name", "Replicating"), noscale, ("dividing", True))


def Divided() -> PartitionType:
    return deepcopy(DIVIDED)


def Reducing(op: Callable) -> PartitionType:
    return PartitionType(
        ("name", "Replicating"),
        noscale,
        ("replication", None),
        ("reducer", to_py_value(op)),
        ("with_key", False),
    )


def ReducingWithKey(op: Callable) -> PartitionType:
    return PartitionType(
        ("name", "Replicating"),
        noscale,
        ("replication", None),
        ("reducer", to_py_value(op)),
        ("with_key", True),
    )


def Distributing() -> PartitionType:
    return PartitionType(("name", "Distributing"))


BLOCKED = PartitionType(("name", "Distributing"), ("distribution", "blocked"))


def BlockedAlong() -> PartitionType:
    return deepcopy(BLOCKED)


@dispatch
def BlockedAlong(along: int) -> PartitionType:
    return PartitionType(
        "name",
        "Distributing",
        ("distribution", "blocked"),
        ("key", along),
    )


@dispatch
def BlockedAlong(along: int, balanced: bool) -> PartitionType:
    return PartitionType(
        ("name", "Distributing"),
        ("distribution", "blocked"),
        ("key", along),
        ("balanced", balanced),
    )


@dispatch
def GroupedBy(key):
    return PartitionType(
        ("name", "Distributing"), ("distribution", "grouped"), ("key", key)
    )


@dispatch
def GroupedBy(key, balanced: bool):
    return PartitionType(
        ("name", "Distributing"),
        ("distribution", "grouped"),
        ("key", key),
        ("balanced", balanced),
    )


def scale_by_future(as_future: Future):
    return lambda f: Scale(f, by=1.0, relative_to=[as_future])


def ScaledBySame(as_future: Future) -> PartitionType:
    return PartitionType(scale_by_future(as_future))


DRIFTED = PartitionType(("name", "Distributing"), ("id", "!"))


def Drifted() -> PartitionType:
    return deepcopy(DRIFTED)


BALANCED = PartitionType(("name", "Distributing"), ("balanced", True), noscale)


def Balanced() -> PartitionType:
    return deepcopy(BALANCED)


UNBALANCED = PartitionType(("name", "Distributing"), ("balanced", False))


def Unbalanced() -> PartitionType:
    return deepcopy(UNBALANCED)


def Unbalanced(scaled_by_same_as: Future) -> PartitionType:
    return PartitionType(
        ("name", "Distributing"),
        ("balanced", False),
        scale_by_future(scaled_by_same_as),
    )


# These functions (along with `keep_sample_rate`) allow for managing memory
# usage in annotated code. `keep_sample_rate` allows for setting the sample
# rate as it changes from value to value. Some operations such as joins
# actually require a change in sample rate so propagating this information is
# important and must be done before partition annotations are applied (in
# `partitioned_using`). In the partition annotation itself, we sometimes want
# to set constraints on how we scale the memory usage based on how much skew
# is introduced by an operation. Some operations not only change the sample
# rate but also introduce skew and so applying these constraints is important.
# FilteredTo and FilteredFrom help with constraining skew when it is introduced
# through data filtering operations while MutatedTo and MutatedFrom allow for
# propagatng skew for operations where the skew is unchanged. Balanced data
# doesn't have any skew and Balanced and balanced=true help to make this clear.


def _distributed(
    samples_for_grouping: SampleForGrouping,
    balanced_res: List[bool],
    filtered_relative_to_res: List[SampleForGrouping],
    filtered_from_res: List[Future],
    filtered_to_res: List[Future],
    filtered_from: bool,
    scaled_by_same_as_res: List[Future],
    rev: bool,
    rev_is_nothing: bool,
) -> List[PartitionType]:
    blocked_pts = make_blocked_pts(
        samples_for_grouping.future,
        samples_for_grouping.axes,
        balanced_res,
        filtered_from_res,
        filtered_to_res,
        scaled_by_same_as_res,
    )
    grouped_pts = make_grouped_pts(
        samples_for_grouping,
        balanced_res,
        rev,
        rev_is_nothing,
        filtered_relative_to_res,
        filtered_from,
        scaled_by_same_as_res,
    )
    return [blocked_pts, grouped_pts]


def Distributed(
    samples_for_grouping: SampleForGrouping,
    # Parameters for splitting into groups
    balanced: bool = None,
    rev: bool = None,
    # Options to deal with skew
    filtered_relative_to: Union[SampleForGrouping, List[SampleForGrouping]] = [],
    filtered_from=False,
    scaled_by_same_as: Future = None,
) -> List[PartitionType]:
    balanced_res = [False, True] if (balanced is None) else [balanced]
    filtered_relative_to_res = (
        filtered_relative_to
        if isinstance(filtered_relative_to, list)
        else [filtered_relative_to]
    )
    filtered_from_res = (
        [filtered_relative_to_res[0].future]
        if (filtered_from and (len(filtered_relative_to_res) != 0))
        else []
    )
    filtered_to_res = (
        [filtered_relative_to_res[0].future]
        if (not filtered_from and (len(filtered_relative_to_res) != 0))
        else []
    )
    scaled_by_same_as_res = (
        [scaled_by_same_as] if (scaled_by_same_as is not None) else []
    )
    res = _distributed(
        samples_for_grouping,
        balanced_res,
        filtered_relative_to_res,
        filtered_from_res,
        filtered_to_res,
        filtered_from,
        scaled_by_same_as_res,
        False if (rev is None) else rev,
        rev is None,
    )
    return res


def get_factor_for_blocked(
    f_s: Sample, filtered: List[Future], filtered_from: bool
) -> float:
    factor = -1.0
    for ff in filtered:
        ff_factor = get_location(ff).sample.memory_usage / f_s.memory_usage
        ff_factor_relative = ff_factor if filtered_from else 1 / ff_factor
        factor = max(factor, ff_factor_relative)
    if factor == -1:
        raise Exception("Factor of scaling should not be -1")
    return factor


def make_blocked_pt(
    f: Future,
    axis: int,
    filtered_from: List[Future],
    filtered_to: List[Future],
    scaled_by_same_as: List[Future],
) -> PartitionType:
    # Initialize parameters
    new_pt = BlockedAlong(axis, False)
    constraints = new_pt.constraints.constraints

    # Create `ScaleBy` constraints
    filtered_relative_to = filtered_from if (len(filtered_from) != 0) else filtered_to
    if len(filtered_relative_to) != 0:
        # If 100 elements get filtered to 20 elements and the
        # original data was block-partitioned in a balanced
        # way, the result may all be on one partition in the
        # msot extreme case (not balanced at all) and so we
        # should adjust the memory usage of the result by
        # multiplying it by the size of the original / the size
        # of the result (100 / 20 = 5).

        # The factor will be infinite or NaN if either what we are
        # filtering from or filtering to has an empty sample. In
        # that case, it wouldn't make sense to have a `ScaleBy`
        # constraint. A value with an empty sample must either be
        # replicated (if it is from an empty dataset) or
        # grouped/blocked but not balanced (since if it were
        # balanced, we might try to use its divisions - which would
        # be empty - for other PTs and think that they to are balanced).
        factor = get_factor_for_blocked(
            get_location(f).sample, filtered_relative_to, (len(filtered_from) != 0)
        )
    elif len(scaled_by_same_as) != 0:
        factor = 1.0
    else:
        factor = float("nan")

    if (not math.isinf(factor)) and (not math.isnan(factor)):
        c_relative_to = (
            filtered_relative_to
            if (len(filtered_relative_to) != 0)
            else scaled_by_same_as
        )
        c = Scale(f, by=factor, relative_to=c_relative_to)
        constraints.append(c)

    return new_pt


def make_blocked_pts(
    f: Future,
    along: List[int],
    balanced: List[bool],
    filtered_from: List[Future],
    filtered_to: List[Future],
    scaled_by_same_as: List[Future],
) -> List[PartitionType]:
    # Create PTs for each axis that can be used to block along
    pts = []
    for axis in along[: min(4, len(along))]:
        # Handle combinations of `balanced` and `filtered_from`/`filtered_to`
        for b in balanced:
            # Append new PT to PT union being produced
            # If the future is the result of reading from some remote location, even
            # if it's unbalanced, it is probably actually balanced because most reading
            # PFs will read in data in a balanced manner.
            if b or (get_location(f).src_name == "Remote"):
                new_pt = BlockedAlong(axis, b)
                new_pt.constraints.constraints.append(noscale(f))
                pts.append(new_pt)
            else:
                pts.append(
                    make_blocked_pt(
                        f, axis, filtered_from, filtered_to, scaled_by_same_as
                    )
                )

    # Return the resulting PT union that can then be passed into a call to `pt`
    # which would in turn result in a PA union
    return pts


def Blocked(
    f: Future,
    along: List[int] = None,
    balanced: bool = None,
    filtered_from: Future = None,
    filtered_to: Future = None,
    scaled_by_same_as: Future = None,
) -> List[PartitionType]:

    if along is None:
        along = sample_axes(sample(f))

    balanced_res = [False, True] if (balanced is None) else [balanced]

    filtered_from_res = [] if (filtered_from is None) else [filtered_from]
    filtered_to_res = [] if (filtered_to is None) else [filtered_to]
    scaled_by_same_as_res = [] if (scaled_by_same_as is None) else [scaled_by_same_as]

    # Create PTs for each axis that can be used to block along
    return make_blocked_pts(
        f,
        along,
        balanced_res,
        filtered_from_res,
        filtered_to_res,
        scaled_by_same_as_res,
    )


# NOTE: A reason to use Grouped for element-wise computation (with no
# filtering) is to allow for the input to be re-balanced. If you just use
# Any then there wouldn't be any way to re-balance right before the
# computation. Grouped allows the input to have either balanced=true or
# balanced=false and if balanced=true is chosen then a cast may be applied.

FutureByOptionalKey = Dict[Future, List]


def _get_factor(factor: float, f: tuple, frt: tuple) -> float:
    f_sample, key = f
    frt_sample, frtkey = frt
    # IF what wea re filtering to is empty, we don't know
    # anything about the skew of data being filtered.
    # Everything could be in a single partition or evenly
    # distributed and the result would be the same. If this
    # PT is ever fused with some matching PT that _is_
    # balanced and has divisions specified, then that will
    # be becasue of some filtering going on in which case a
    # `ScaleBy` constraint will be enforced.

    # Handling empty data:
    # If empty data is considered balanced, we should
    # replicate everything. Otherwise, there should be some
    # other data in the filtering pipeline that is
    # balanced.
    # - disallow balanced grouping of empty dataset
    # - filtering to an empty dataset - no ScaleBy constraint
    # - filtering from an empty dataset - no ScaleBy constraint
    # - maybe add in a ScaleBy(-1j) to prevent usage of an empty data

    # Compute the amount to scale memory usage by based on data skew
    min_frt = sample_min(f_sample, key)
    max_frt = sample_max(f_sample, key)
    # divisions_filtered_from = sample(ff, :statistics, key, :divisions)
    frt_percentile = sample_percentile(frt_sample, frtkey, min_frt, max_frt)
    frtfactor = 1.0 / frt_percentile
    return max(factor, frtfactor)


def get_factor(
    factor: float, f_sample, key, frt_samples: SampleForGrouping, filtered_from: bool
) -> float:
    # We want to find the factor of scaling to account for data skew when
    # filtering from/to some other future. So
    f_pair = (f_sample, key)
    frt_sample = frt_samples.sample
    if key in frt_samples.keys:
        frt_pair = (frt_sample, key)
    else:
        frt_pair = (frt_sample, frt_samples.keys[0])
    res = _get_factor(
        factor,
        f_pair if filtered_from else frt_pair,
        frt_pair if filtered_from else f_pair,
    )
    return res


def make_grouped_balanced_pt(
    samples_for_grouping: SampleForGrouping,
    key,
    rev: bool,
    rev_is_nothing: bool,
) -> PartitionType:
    new_pt = GroupedBy(key, True)
    parameters = new_pt.parameters
    constraints = new_pt.constraints.constraints

    f = samples_for_grouping.future
    f_sample = samples_for_grouping.sample

    # Set divisions
    # TODO: Change this if `divisions` is not a `Vector{Tuple{Any,Any}}`
    sampled_divisions = sample_divisions(f_sample, key)
    parameters["divisions"] = to_py_value(sampled_divisions)
    max_ngroups = sample_max_ngroups(f_sample, key)

    # Set flag for reversing the order of the groups
    if not rev_is_nothing:
        parameters["rev"] = rev

    # Add constraints
    # In the future if the element size can be really big (like a multi-dimensional array
    # that is very wide or data frame with many columns), we may want to have a constraint
    # where the partition size must be larger than that minimum element size.

    # Note that if the sample is empty, the maximum # of groups
    # will be zero and so this AtMost constraint will cause the PA
    # to fail to be used. This is expected. You can't have a PA
    # where empty data is balanced. If the empty data arises
    # because of an empty dataset being queried/processed, we
    # should be using replication. If the empty data arises because
    # of highly selective filtering, we will filter from some data
    # that _is_ balanced.
    constraints.append(AtMost(max_ngroups, f))
    constraints.append(Scale(f, by=1.0))

    # TODO: Make AtMost only accept a value (we can support PT references in the future if needed)
    # TODO: Make scheduler check that the values in AtMost or ScaledBy are actually present to ensure
    # that the constraint can be satisfied for this PT to be used

    return new_pt


def make_grouped_filtered_pt(
    samples_for_grouping: SampleForGrouping,
    key,
    filtered_relative_to: List[SampleForGrouping],
    filtered_from: bool,
) -> PartitionType:
    new_pt = GroupedBy(key, False)

    # Create `ScaleBy` constraint and also compute `divisions` and
    # `AtMost` constraint if balanced

    # TODO: Support joins
    factor = 0.0
    f_sample = samples_for_grouping.sample
    relative_to = []
    for frt in filtered_relative_to:
        relative_to.append(frt.future)
        factor = get_factor(factor, f_sample, key, frt, filtered_from)

    # If the sample of what we are filtering into is empty, the
    # factor will be infinity. In that case, we shouldn't be
    # creating a ScaleBy constraint.
    f = samples_for_grouping.future
    if not math.isinf(factor):
        new_pt.constraints.constraints.append(
            Scale(f, by=factor, relative_to=relative_to)
        )

    return new_pt


def make_grouped_pt(
    f: Future,
    key,
    scaled_by_same_as: List[Future],
):
    new_pt = GroupedBy(key, False)
    new_pt.constraints.constraints.append(
        Scale(f, by=1.0, relative_to=scaled_by_same_as)
    )
    return new_pt


NONE = PartitionType(
    ("key", ""), ("distribution", ""), ("balanced", False), lambda f: AtMost(0, f)
)


def make_grouped_pts(
    # This parameter is passed in just so that we can have type stability in
    # `f_sample` local variable
    samples_for_grouping: SampleForGrouping,
    balanced: List[bool],
    rev: bool,
    rev_isnothing: bool,
    filtered_relative_to: List[SampleForGrouping],
    filtered_from: bool,
    scaled_by_same_as: List[Future],
) -> List[PartitionType]:
    # Create PTs for each key that can be used to group by
    pts = []
    for key in samples_for_grouping.keys:
        # Handle combinations of `balanced` and `filtered_from`/`filtered_to`
        for b in balanced:
            if b:
                pts.append(
                    make_grouped_balanced_pt(
                        samples_for_grouping, key, rev, rev_isnothing
                    )
                )
            elif len(filtered_relative_to) != 0:
                pts.append(
                    make_grouped_filtered_pt(
                        samples_for_grouping, key, filtered_relative_to, filtered_from
                    )
                )
            elif len(scaled_by_same_as) != 0:
                pts.append(
                    make_grouped_pt(samples_for_grouping.future, key, scaled_by_same_as)
                )

    # If there are no PTs, ensure that we at least have one impossible PT. This
    # PT doesn't need to have divisions and can be unbalanced but the important
    # thing is that we assign an AtMost-zero constraint which will prevent a PA
    # containing this from being used. This is important because we can't group
    # data on keys that don't belong to it.
    if len(pts) == 0:
        pts.append(deepcopy(NONE))

    return pts


def Grouped(
    samples_for_grouping: SampleForGrouping,
    # Parameters for splitting into groups
    balanced: bool = None,
    rev: bool = None,
    # Options to deal with skew
    filtered_relative_to: Union[SampleForGrouping, List[SampleForGrouping]] = [],
    filtered_from: bool = False,
    scaled_by_same_as: Future = None,
) -> List[PartitionType]:
    make_grouped_pts(
        samples_for_grouping,
        [False, True] if (balanced is None) else [balanced],
        False if (rev is None) else rev,
        rev is None,
        filtered_relative_to
        if isinstance(filtered_relative_to, list)
        else [filtered_relative_to],
        filtered_from,
        [] if (scaled_by_same_as is None) else [scaled_by_same_as],
    )


# sample_for_grouping called from annotation code on the main future as well as all things being filtered to/from
# T, K, TF, KF
# make_grouped_pts should call make_grouped_pt on each grouping key from the sample
# make_grouped_pt shuld have a special case for where the key types are the same - try to use the right sample; otherwise just use any sample
# make_grouped_pt should be broken into helper functions that don't rely on T and maybe just rely on K

# Three use-cases of Grouped / filtered_from/filtered_to:
# - pts_for_filtering for getindex - sample of filtered_to can be different and any key
# - Grouped with filtered_to for innerjoin - key is specified for each future
