from copy import deepcopy
import inspect
import logging
from math import ceil
from textwrap import dedent
from typing import Any, Dict, List, Tuple, Union

from plum import dispatch

from .future import Future, is_sample_memory_usage_known
from .futures import get_location
from .locations import apply_sourced_or_destined_funcs
from .partitions import Cross, Match, MatchOn
from .request import RecordTaskRequest
from .requests import record_request
from .sample import Sample
from .samples import keep_same_statistics, sample, sample_keys, set_sample
from .sessions import get_session
from .task import DelayedTask
from .utils_partitions import (
    PartitionAnnotation,
    PartitionType,
    PartitioningConstraint,
    PartitioningConstraintOverGroup,
    PartitioningConstraintOverGroups,
    PartitioningConstraints,
    PartitionTypeComposition,
    PartitionTypeReference,
    PartitionedUsingFunc,
)

###############################
# Global state for annotation #
###############################

curr_delayed_task = DelayedTask()


def set_task(t: DelayedTask):
    global curr_delayed_task
    curr_delayed_task = t


def get_task() -> DelayedTask:
    global curr_delayed_task
    return curr_delayed_task


def finish_task():
    set_task(DelayedTask())


def get_pa() -> PartitionAnnotation:
    get_task().pa_union[-1]


def finish_pa():
    curr_delayed_task = get_task()
    curr_delayed_task.pa_union = curr_delayed_task.pa_union.append(
        PartitionAnnotation()
    )


#################################################
# Setting up sample properties in beforecompute #
#################################################

# Comments copied from Banyan.jl
#
# These functions are necessary because when we construct PT's to assign to
# futures for annotated code (especially code in BDF.jl), we rely on statistics
# of the samples of the futures to construct the PTs. But computing statistics
# is expensive and we only want to compute it for the keys/columns that are
# actually used for grouping/sorting/etc. So we propagate this through the
# `:groupingkeys` sample property of different futures (each future has a
# set of sample properties).
#
# These `keep_*` functions specify how `statistics` and `groupingkeys` sample
# properties should propagate. These functions are called inside of a
# `partitioned_using` so that they are called in a forward pass through all the
# tasks (code regions) detected and also in a backward pass to properly
# propagate the `groupingkeys` sample property. For example, a groupby may only
# happen in the very last task indicating that a particular column can be in
# the `groupingkeys` (the column used for the groupby) but we need to specify
# that that column could have been used for grouping the data throughout (if
# all tasks are amenable like that).
#
# Of course, columns can be dropped or
# renamed and so that makes things a bit challenging in properly propagating
# information about sample properties and that's why we have a lot of these
# keep_* functions.


def _get_new_p_groupingkeys(
    p: Future,
    p_s: Sample,
    participants: List[Future],
    drifted: bool,
    p_keys: list,
    keeping_same_statistics: list,
):
    new_p_groupingkeys = []
    for op in participants:
        op_groupingkeys = get_location(op).sample.groupingkeys
        for op_groupingkey in op_groupingkeys:
            if (op_groupingkey in p_keys) and (
                not (op_groupingkey in new_p_groupingkeys)
            ):
                new_p_groupingkeys = new_p_groupingkeys.append(op_groupingkey)

                # Only allow keys that are actually in the keys of the participants
                # TODO: Maybe replace this with a function that doesn't require calling
                # a potentially expensive function to iterate through _all_ columns.
                if not drifted:
                    keeping_same_statistics = keeping_same_statistics.append(
                        (p, op_groupingkey, op, op_groupingkey)
                    )
    p_s.groupingkeys = new_p_groupingkeys


def get_new_p_groupingkeys(
    p: Future,
    p_s: Sample,
    participants: List[Future],
    drifted: bool,
    p_sample,
    keeping_same_statistics: List[tuple],
):
    _get_new_p_groupingkeys(
        p, p_s, participants, drifted, sample_keys(p_sample), keeping_same_statistics
    )


def keep_all_sample_keys(
    participants: List[Future], drifted: bool, keeping_same_statistics: List[tuple]
):
    if not participants:
        return

    # This can be use on an input and output that share the same keys that they
    # can be grouped by.

    # Take the union of discovered grouping keys. Grouping keys are discovered
    # through calls to `keep_keys` in functions where we know that we need
    # to allow grouping by certain keys such as sorting and join functions.
    for p in participants:
        p_s: Sample = get_location(p).sample
        p_sample = p_s.value
        get_new_p_groupingkeys(
            p, p_s, participants, drifted, p_sample, keeping_same_statistics
        )


def apply_keeping_same_statistics(keeping_same_statistics: List[tuple]):
    for same in keeping_same_statistics:
        keep_same_statistics(same[1], same[2], same[3], same[4])


def keep_all_sample_keys_renamed(
    o: Tuple[Future, Sample, Any],
    n: Tuple[Future, Sample, Any],
    keeping_same_statistics: List[tuple],
):
    if isinstance(o, Future) and isinstance(n, Future):
        o_s = get_location(o).sample
        n_s = get_location(o).sample
        o = (old, o_s, o_s.value)
        n = (new, n_s, n_s.value)
    elif not (isinstance(o[2], list) and isinstance(isinstance(n[2], list))):
        # The type of o and n would be tuple{Future,Sample,T}, where T is generic type
        o = (o[0], o[1], sample_keys(o[2]))
        n = (n[0], n[1], sample_keys(n[2]))
    old, old_sample, old_keys = o
    new, new_sample, new_keys = n
    old_groupingkeys: list = old_sample.groupingkeys
    old_groupingkeys_changed = False
    new_groupingkeys: list = new_sample.groupingkeys
    new_groupingkeys_changed = False
    if len(old_keys) != len(new_keys):
        raise Exception(
            "Expected renaming operation to not change the number of keys/axes/columns of this data"
        )
    for i in range(1, len(old_keys)):
        ok = old_keys[i]
        nk = new_keys[i]
        if ok in old_groupingkeys:
            new_groupingkeys = new_groupingkeys.append(nk)
            keeping_same_statistics = keeping_same_statistics.append((new, nk, old, ok))
            new_groupingkeys_changed = True
        if nk in new_groupingkeys:
            old_groupingkeys = old_groupingkeys.append(ok)
            keeping_same_statistics = keeping_same_statistics.append((old, ok, new, nk))
            old_groupingkeys_changed = True
    if new_groupingkeys_changed:
        new_sample.groupingkeys = new_groupingkeys
    if old_groupingkeys_changed:
        old_sample.groupingkeys = old_groupingkeys


def keep_sample_keys_named(
    participants: List[Tuple[Future, list]],
    drifted: bool,
    keeping_same_statistics: List[tuple],
):
    # `participants` maps from futures to lists of key names such that all
    # participating futures have the same sample properties for the keys at
    # same indices in those lists
    first_participant_pair = participants[0]
    first_participant: Future = first_participant_pair[0]
    first_keys: list = first_participant_pair[1]
    nkeys = len(first_keys)
    for i in range(nkeys):
        # Copy over allowed grouping keys
        for (p, keys) in participants:
            p_key = keys[i]
            p_sample = get_location(p).sample
            p_sample_groupingkeys = p_sample.groupingkeys
            if p_key not in p_sample_groupingkeys:
                p_sample_groupingkeys = p_sample_groupingkeys.append(p_key)
            p_sample.groupingkeys = p_sample_groupingkeys

            # Copy over statistics if they haven't changed
            if not drifted:
                for gk in p_sample_groupingkeys:
                    keeping_same_statistics = keeping_same_statistics.append(
                        (p, gk, first_participant, first_keys[i])
                    )


# NOTE: For operations where join rate and data skew might be different, it is
# assumed that they are the same. For example, column vectors from the result
# of a join should have the same sample rate and the same data skew.


def keep_sample_keys(
    keys: list,
    participants: List[Future],
    drifted: bool,
    keeping_same_statistics: List[tuple],
):
    return keep_sample_keys_named(
        [(p, keys) for p in participants], drifted, keeping_same_statistics
    )


# This is useful for workloads that involve joins where the sample rate is
# diminished quadratically for each joinv
# TODO: Make _this_ more accurate for `innerjoin``
def keep_sample_rate(fut: Future, relative_to: Union[Future, List[Future]]):
    if isinstance(relative_to, Future):
        fut_sample: Sample = get_location(fut).sample
        relative_to_sample: Sample = get_location(relative_to).sample
        fut_sample.rate = relative_to_sample.rate
    elif isinstance(relative_to, List[Future]):
        rate_prod = 1
        for r in relative_to:
            rate_prod = rate_prod * get_location(r).sample.rate
        get_location(fut).sample.rate = rate_prod
    else:
        raise ValueError("relative_to must either be a Future or list of Futures")


###############################
# Using samples to assign PTs #
###############################


def get_inputs_and_outputs(
    curr_delayed_task_mutation: Dict[Future, Future],
    curr_delayed_task_scaled: List[Future],
    grouping_needed: bool,
    grouped: List[Future],
) -> Tuple[List[Future], List[Future], List[Future], List[Future]]:
    output_value_ids = []
    for ff in curr_delayed_task_mutation.values():
        output_value_ids = output_value_ids.append(ff.value_id)
    outputs = []
    inputs = []
    for f in curr_delayed_task_scaled:
        if f.value_id in output_value_ids:
            outputs.append(f)
        else:
            inputs = inputs.append(f)

    outputs_grouped = []
    inputs_grouped = []
    if grouping_needed:
        for f in grouped:
            if f.value_id in output_value_ids:
                outputs_grouped = outputs_grouped.append(f)
            else:
                inputs_grouped = inputs_grouped.append(f)

    return outputs, inputs, outputs_grouped, inputs_grouped


def apply_partitioned_using_func_for_sample_rates(
    inputs: List[Future], keep_same_sample_rate: bool, outputs: List[Future]
):
    if inputs:
        if keep_same_sample_rate:
            for r in outputs:
                keep_sample_rate(r, inputs[0])
            for i in range(len(inputs) - 1):
                this_sample_rate = get_location(inputs[i]).sample.rate
                other_sample_rate = get_location(inputs[i + 1]).sample.rate
                if this_sample_rate != other_sample_rate:
                    logging.warn(
                        "Two inputs have different sample rates ($this_sample_rate, $other_sample_rate)"
                    )
        else:
            for r in outputs:
                keep_sample_rate(r, inputs)


def apply_partitioned_using_func(f: PartitionedUsingFunc):
    keep_same_sample_rate = f.keep_same_sample_rate
    # Keys (not relevant if you never use grouped partitioning).
    grouped = f.grouped
    keep_same_keys = f.keep_same_keys
    keys = f.keys
    keys_by_future = f.keys_by_future
    renamed = f.renamed
    # Asserts that output has a unique partitioning compared to inputs
    # (not relevant if you never have unbalanced partitioning)
    drifted = f.drifted

    # Categorize the futures. We determine whether a future is an input by
    # checking if it's not in the values of the task's mutation. All
    # outputs would require mutation for the future to be an output. And
    # that would have been marked by a call to `mutated` that is made in the
    # `Future` constructor.
    grouping_needed = (
        keep_same_keys or (len(keys) != 0) or (len(keys_by_future) != 0) or renamed
    )
    curr_delayed_task = get_task()
    curr_delayed_task_mutation = curr_delayed_task.mutation
    curr_delayed_task_scaled = curr_delayed_task.scaled
    outputs, inputs, outputs_grouped, inputs_grouped = get_inputs_and_outputs(
        curr_delayed_task_mutation, curr_delayed_task_scaled, grouping_needed, grouped
    )

    # Propagate information about keys that can be used for grouping
    if grouping_needed:
        keeping_same_statistics = []
        if keep_same_keys:
            if renamed:
                if (len(inputs_grouped) != 1) or (len(outputs_grouped) != 1):
                    raise Exception(
                        "Only 1 argument can be renamed to 1 result at once"
                    )
                keep_all_sample_keys_renamed(
                    inputs_grouped[0], outputs_grouped[0], keeping_same_statistics
                )
            else:
                keep_all_sample_keys(grouped, drifted, keeping_same_statistics)
        if keys:
            keep_sample_keys(keys, grouped, drifted, keeping_same_statistics)
        if keys_by_future:
            keep_sample_keys_named(keys_by_future, drifted, keeping_same_statistics)
        if keeping_same_statistics:
            apply_keeping_same_statistics(keeping_same_statistics)

    # Propgate sample rates
    apply_partitioned_using_func_for_sample_rates(
        inputs, keep_same_sample_rate, outputs
    )

    # Store the important inputs and outputs for scaling memory usage
    curr_delayed_task.inputs = inputs
    curr_delayed_task.outputs = outputs


def _partitioned_with(
    handler,  # function
    futures: List[Future],
    # Memory usage, sampling
    # `scaled` is the set of futures with memory usage that can potentially be
    # scaled to larger sizes if the amount of data at a location changes.
    # Non-scaled data has fixed memory usage regardless of its sample rate.
    scaled: List[Future],
    keep_same_sample_rate: bool,
    memory_usage: List[PartitioningConstraint],
    additional_memory_usage: List[PartitioningConstraint],
    # Keys (not relevant if you never use grouped partitioning).
    grouped: List[Future],
    keep_same_keys: bool,
    keys: list,
    keys_by_future: List[Tuple[Future, list]],
    renamed: bool,
    # Asserts that output has a unique partitioning compared to inputs
    # (not relevant if you never have unbalanced partitioning)
    drifted: bool,
    # For generating import statements
    modules: List[str],
):
    curr_delayed_task: DelayedTask = get_task()

    if modules:
        partitioned_using_modules(modules)

    curr_delayed_task.futures = futures
    curr_delayed_task.scaled = scaled
    curr_delayed_task.partitioned_with_func = handler
    curr_delayed_task.keep_same_sample_rate = keep_same_sample_rate
    curr_delayed_task.memory_usage_constraints = memory_usage
    curr_delayed_task.additional_memory_usage_constraints = additional_memory_usage

    # TODO: Ensure partitioned_using is able to capture updates to the task when mutating

    curr_delayed_task.partitioned_using_func = PartitionedUsingFunc(
        keep_same_sample_rate,
        grouped,
        keep_same_keys,
        keys,
        keys_by_future,
        renamed,
        drifted,
        False,
    )


def partitioned_with(
    handler,
    futures: List[Future],
    # Memory usage, sampling
    # `scaled` is the set of futures with memory usage that can potentially be
    # scaled to larger sizes if the amount of data at a location changes.
    # Non-scaled data has fixed memory usage regardless of its sample rate.
    scaled: List[Future] = [],
    keep_same_sample_rate: bool = True,
    memory_usage: List[PartitioningConstraint] = [],
    additional_memory_usage: List[PartitioningConstraint] = [],
    # Keys (not relevant if you never use grouped partitioning).
    grouped: List[Future] = [],
    keep_same_keys: bool = False,
    keys: list = None,
    keys_by_future: List[Tuple[Future, list]] = None,
    renamed: bool = False,
    # Asserts that output has a unique partitioning compared to inputs
    # (not relevant if you never have unbalanced partitioning)
    drifted: bool = False,
    # For generating import statements
    modules: List[str] = [],
    keytype=int,
):
    # scaled
    scaled_res = scaled

    # grouped
    grouped_res = scaled_res if (len(grouped) == 0) else grouped

    # modules
    modules_res = modules if isinstance(modules, list) else [modules]

    return _partitioned_with(
        handler,
        futures,
        scaled_res,
        keep_same_sample_rate,
        memory_usage,
        additional_memory_usage,
        grouped_res,
        keep_same_keys,
        [] if (keys is None) else keys,
        [] if (keys_by_future is None) else keys_by_future,
        renamed,
        drifted,
        modules_res,
    )


def evaluate_constraint(
    constraint: PartitioningConstraint, fut: Future
) -> PartitioningConstraint:
    constraint_func = constraint.func
    if constraint_func != (lambda x: x):
        return constraint_func(fut)
    else:
        return constraint


def evaluate_constraint_func(fut: Future):
    return lambda constraint: evaluate_constraint(constraint, fut)


def pt_partition_type_composition(
    fut: Future,
    ptype: PartitionTypeComposition,
    to_match: List[Future],
    on: List[str],
    to_cross: List[Future],
):

    # Start new PA if this assignment would overwrite one for the current
    # PA. When we start a new PA, we append the old one the PA union for
    # the task being currently constructed.
    current_pa = get_pa()
    if current_pa.partitions.pt_stacks.haskey(fut.value_id):
        finish_pa()
        pa = get_pa()
    else:
        pa = current_pa

    # Handle constraints that have been delayed till PT assignment
    efunc = evaluate_constraint_func(fut)
    for pty in ptype.pts:
        new_constraints = list(map(efunc, pty.constraints.constraints))
        pty.constraints.constraints = new_constraints

    # Add to PAs information about how partitions are produced
    pa.partitions.pt_stacks[fut.value_id] = ptype

    # Handle `match`, `on` in keyword arguments
    if to_match:
        for to_match_with in to_match:
            fut_and_to_match_with = [fut, to_match_with]
            if on:
                for to_match_on in on:
                    pa.constraints.constraints = pa.constraints.constraints.append(
                        MatchOn(to_match_on, fut_and_to_match_with),
                    )
            else:
                pa.constraints.constraints = pa.constraints.constraints.append(
                    Match(fut_and_to_match_with)
                )

    if to_cross:
        pa.constraints.constraints = pa.constraints.constraints.append(Cross(to_cross))


def pt_partition_type(
    ptype: PartitionType,
    futs: List[Future],
    match: List[Future],
    on: List[str],
    cross: List[Future],
):
    pt_composition = PartitionTypeComposition([ptype])
    for fut in futs:
        pt_partition_type_composition(fut, deepcopy(pt_composition), match, on, cross)


def pt_partition_type(
    ptype: PartitionTypeComposition,
    futs: List[Future],
    match: List[Future],
    on: List[str],
    cross: List[Future],
):
    for fut in futs:
        pt_partition_type_composition(fut, ptype, match, on, cross)


def pt_partition_type(
    ptype: List[PartitionType],
    futs: List[Future],
    match: List[Future],
    on: List[str],
    cross: List[str],
):
    for fut in futs:
        for i in range(len(ptype)):
            pt_composition = PartitionTypeComposition(ptype[i : i + 1])
            pt_partition_type_composition(fut, pt_composition, match, on, cross)


def pt(
    # # args::Union{Future,PartitionType,PartitionTypeComposition,Vector{PartitionType}}...;
    # args...;
    # match = nothing,
    # on = nothing,
    # cross = nothing
    # args::Union{Future,PartitionType,PartitionTypeComposition,Vector{PartitionType}}...;
    *args,
    match: Future = None,
    on: Union[str, List[str]] = [],
    cross: List[Future] = []
    # args::Union{AbstractFuture,PartitionType,PartitionTypeComposition,Vector{PartitionType}}...;
    # match::Union{Nothing,AbstractFuture} = nothing,
    # on::Union{String,Vector{String}} = String[],
    # cross::Vector{<:AbstractFuture} = Future[]
):
    if len(args) < 1:
        raise ValueError(
            "Cannot assign partition type with `pt` unless at least one argument is passed in"
        )

    futs_res = list(args[:-1])
    match_res = [] if (match is None) else [match]
    on_res = [] if (on is None) else ([on] if isinstance(on, str) else on)
    cross_res = [] if (cross is None) else cross

    ptype = deepcopy(args[-1])
    if len(futs_res) > 0:
        pt_partition_type(ptype, futs_res, match_res, on_res, cross_res)


# NOTE: `mutated` should not be used inside of `partitioned_with` or
# `partitioned_using`


@dispatch
def mutated(old: Future, new: Future):
    # if haskey(curr_delayed_task.value_names, old.value_id)
    #     finish_pa()
    t = get_task()
    t.mutation[old] = new


def mutated(f: Future):
    mutated(f, f)


def mutated(ff: Tuple[Future, Future]):
    mutated(ff.first, ff.second)


#################################################
# Macro for wrapping the code region to offload #
#################################################


@dispatch
def apply_mutation(old: Future, new: Future):
    # Apply the mutation by setting the value ID of the old future the
    # value ID of the new one. That way, the user can continue using
    # the old future as if it were mutated but it will be having a
    # different value ID.

    # Swap (1) references in `futures_on_client` if either side of the
    # mutation is on the client
    futures_on_client = get_session().futures_on_client
    old_on_client = futures_on_client.haskey(old.value_id)
    new_on_client = futures_on_client.haskey(new.value_id)
    if old_on_client and new_on_client:
        futures_on_client[new.value_id], futures_on_client[old.value_id] = (
            futures_on_client[old.value_id],
            futures_on_client[new.value_id],
        )
    elif old_on_client:
        futures_on_client[new.value_id] = futures_on_client[old.value_id]
        futures_on_client.pop(old.value_id)
    elif new_on_client:
        futures_on_client[old.value_id] = futures_on_client[new.value_id]
        futures_on_client.pop(new.value_id)

    # Swap (2) other fields of the `Future`s and (3) their locations
    session_locations = get_session().locations
    old.value,
    new.value,
    old.value_id,
    new.value_id,
    old.mutated,
    new.mutated,
    old.stale,
    new.stale,
    old.sample_memory_usage,
    new.sample_memory_usage,
    session_locations[old.value_id],
    session_locations[new.value_id] = (new.value,)
    old.value,
    new.value_id,
    old.value_id,
    new.mutated,
    old.mutated,
    new.stale,
    old.stale,
    new.sample_memory_usage,
    old.sample_memory_usage,
    session_locations[new.value_id],
    session_locations[old.value_id]


@dispatch
def apply_mutation(
    mutation: Dict[Future, Future], inverted: bool
):  # :IdDict{Future,Future}
    for (old, new) in mutation:
        if old != new:  # TODO: previously !== in julia
            if not inverted:
                apply_mutation(old, new)
            else:
                apply_mutation(new, old)


def partitioned_using_modules(m: List[str]):
    curr_delayed_task = get_task()
    curr_delayed_task.used_modules = [*curr_delayed_task.used_modules, *m]


def finish_partitioned_code_region(splatted_futures: List[Future]):
    task: DelayedTask = get_task()

    # Update mutated futures
    for fut in splatted_futures:
        for m in task.mutation.values():
            if fut.value_id == m.value_id:
                fut.stale = True
                fut.mutated = True
                break

    # Apply all delayed source and destination assignment. This will
    # perform any expensive sample collection that may require for example
    # an expensive scan of S3. This will record `RecordLocationRequest`s.
    for splatted_future in splatted_futures:
        apply_sourced_or_destined_funcs(splatted_future)

    # Look at mutation, inputs, outputs, and constraints to determine
    # initial/final/additional memory usage and also to issue destroy
    # requests for all mutated values. Maybe also set to nothing and
    # assign new value here for mutation. Also set new future's total
    # memory usage.

    # Get the initial memory usage
    for fut in splatted_futures:
        if is_sample_memory_usage_known(fut):
            fut_initial_memory_usage = fut.sample_memory_usage
        else:
            try:
                tmu = get_location(fut).sample_memory_usage
            except NameError as e:
                raise Exception(
                    "Future with value ID $(fut.value_id) has no initial memory usage even in location with source name $(get_location(fut).src_name)"
                )
            except:
                tmu = -1
                raise
            fut_initial_memory_usage = tmu
        task.memory_usage[fut.value_id] = {"initial": fut_initial_memory_usage}

    # Get the final memory usage if it is not dependent on a constraint or other sample rates
    for fut in splatted_futures:
        # Figure out if the future is mutated by this code region
        is_fut_mutated = task.effects[fut.value_id] == "MUT"

        # Get the final memory usage
        if not is_fut_mutated:
            # For non-mutated data, we will only look at the initial
            # memory usage (in the scheduler) so it's fine to set the final
            # memory usage to the initial memory usage.
            task.memory_usage[fut.value_id]["final"] = task.memory_usage[fut.value_id][
                "initial"
            ]
        else:
            # Set memory usage based on a ScaleTo constraint if there is on
            final_memory_usage_set = False
            for c in task.memory_usage_constraints:
                if (
                    c.type.startswith("SCALE_TO=")
                    and (len(c.args) == 1)
                    and (c.args[0] == fut.value_id)
                ):
                    final_memory_usage_set = True
                    task.memory_usage[fut.value_id]["final"] = int(
                        c.type[len("SCALE_TO=") + 1 :]
                    )

            # If not and if this isn't scaled, then just set it to the sampled size if the
            # memory usage doesn't scale to larger values
            is_fut_scaled = False
            for f in task.scaled:
                if fut.value_id == f.value_id:
                    is_fut_scaled = True
            if (not final_memory_usage_set) and (not is_fut_scaled):
                task.memory_usage[fut.value_id]["final"] = get_location(
                    fut
                ).sample.memory_usage

    # Apply SCALE_BY constraints to determine final memory usage
    for fut in splatted_futures:
        if not task.memory_usage[fut.value_id].haskey("final"):
            for c in task.memory_usage_constraints:
                if (
                    c.type.startswith("SCALE_BY=")
                    and (len(c.args) == 2)
                    and (c.args[0] == fut.value_id)
                ):
                    relative_to = c.args[1]
                    if task.memory_usage[relative_to].haskey("final"):
                        factor = float(c.type[10:])
                        task.memory_usage[fut.value_id]["final"] = ceil(
                            factor * task.memory_usage[relative_to]["final"]
                        )

    for fut in splatted_futures:
        if not task.memory_usage[fut.value_id].haskey("final"):
            total_sampled_input_memory_usage = 0
            for scaled_fut in task.scaled:
                if task.effects[scaled_fut.value_id] == "CONST":
                    total_sampled_input_memory_usage += get_location(
                        scaled_fut
                    ).sample.memory_usage
            if task.keep_same_sample_rate and (total_sampled_input_memory_usage > 0):
                # This case applies for most computation like `filter` and `groupby`

                total_input_memory_usage = 0
                for fut in task.scaled:
                    if task.effects[fut.value_id] == "CONST":
                        total_input_memory_usage += task.memory_usage[fut.value_id][
                            "initial"
                        ]

                # Use the sampels to figure out the rate of change in
                # memory usage going from inputs to outputs
                factor = (
                    get_location(fut).sample.memory_usage
                    / total_sampled_input_memory_usage
                )

                # Now we use that rate on the actual initial memory
                # usage which might have been modified using past memory
                # usage constraints like ScaleBy and ScaleTo.
                task.memory_usage[fut.value_id]["final"] = int(
                    ceil(factor * total_input_memory_usage)
                )
            elif task.memory_usage[fut.value_id]["initial"] != 0:
                # If the input is nonzero then the output is the same
                # because we don't support a change in memory usage that
                # isn't going from `nothing` to some assigned value.
                # This case applies to the very last code region created in
                # `partitioned_computation`.
                task.memory_usage[fut.value_id]["final"] = task.memory_usage[
                    fut.value_id
                ]["initial"]
            else:
                # This case applies for `fill` and `innerjoin`.
                fut_s = get_location(fut).sample
                task.memory_usage[fut.value_id]["final"] = (
                    fut_s.memory_usage * fut_s.rate
                )

    # Compute additional memory usage
    for fut in splatted_futures:
        additional_memory_usage = 0
        for c in task.additional_memory_usage_constraints:
            if (
                c.type.startswith("SCALE_TO=")
                and (len(c.args) == 1)
                and (c.args[0] == fut.value_id)
            ):
                additional = int(c.type[len("SCALE_TO=") + 1 :])
                additional_memory_usage += additional
            elif (
                c.type.startswith("SCALE_BY=")
                and (len(c.args) == 2)
                and (c.args[0] == fut.value_id)
            ):
                arg = c.args[1]
                factor = float(c.type[len("SCALE_BY=") + 1 :])
                additional_memory_usage += ceil(
                    factor * task.memory_usage[arg]["final"]
                )
        task.memory_usage[fut.value_id]["additional"] = additional_memory_usage

    # Ensure that all the outputs have the same sample rate
    output_sample_rate = -1
    output_sample_rate_from_scaled = False
    is_anything_mutated = False
    for fut in splatted_futures:
        is_fut_mutated = task.effects[fut.value_id] == "MUT"
        is_fut_scaled = False
        for f in task.scaled:
            if fut.value_id == f.value_id:
                is_fut_scaled = True
        is_anything_mutated = is_anything_mutated or is_fut_mutated
        if (not output_sample_rate_from_scaled) and is_fut_mutated:
            output_sample_rate = get_location(fut).sample.rate
            output_sample_rate_from_scaled = is_fut_scaled
    if not ((output_sample_rate != -1) or (not is_anything_mutated)):
        raise Exception("Failed to compute output sample rate")
    for fut in splatted_futures:
        # Skip over non-mutated futures and scaled futures
        if task.effects[fut.value_id] != "MUT":
            continue
        for f in task.scaled:
            if fut.value_id == f.value_id:
                continue
        # Set sample rate for futures that are mutated and not scaled
        # (we already keep the same sample rate for scaled futures)
        get_location(fut).sample.rate = output_sample_rate

    # Destroy value IDs that are no longer needed because of mutation
    for fut in splatted_futures:
        fut.sample_memory_usage = task.memory_usage[fut.value_id]["final"]

        # Issue destroy request for mutated futures that are no longer
        # going to be used
        is_fut_used = False
        fut_value_id = fut.value_id
        for f in task.mutation.keys():
            if fut_value_id == f.value_id:
                is_fut_used = True
        is_fut_to_be_used = False
        for f in task.mutation.values():
            if fut_value_id == f.value_id:
                is_fut_to_be_used = True
        if is_fut_used and (not is_fut_to_be_used):
            destroy_future(fut)

    # Record request to record task in backend's dependency graph and reset
    record_request(RecordTaskRequest(task))
    finish_task()

    # Make a call to `apply_mutation` to handle calls to `mut` like
    # `mutated(df, res)`
    apply_mutation(task.mutation, False)


def get_splatted_futures(
    unsplatted_futures: List[Union[Future, List[Future]]]
) -> List[Future]:
    splatted_futures = []
    for unsplatted_future in unsplatted_futures:
        if isinstance(unsplatted_future, list):
            for uf in unsplatted_future:
                splatted_futures = splatted_futures.append(uf)
        else:
            splatted_futures = splatted_futures.append(unsplatted_future)
    return splatted_futures


def prepare_task_for_partitioned_code_region(
    unsplatted_futures: List[Union[Future, List[Future]]],
    unsplatted_variable_names: List[str],
    splatted_futures: List[Future],
    code: str,
):
    splatted_variable_names = []
    task = get_task()
    # Get code to initialize the unsplatted variable in the code region
    # TODO: Generate code in codegen that puts each code region in a
    # seperate function (where we reuse functions with the hash of the
    # function body) so that we don't have scoping-related bugs
    task.code = ""
    for j in range(len(unsplatted_variable_names)):
        unsplatted_variable_name = unsplatted_variable_names[j]
        task.code += f"{unsplatted_variable_name} = "
        if isinstance(unsplatted_futures[j], list):
            uf = unsplatted_futures[j]
            task.code += "["
            for i in range(len(uf)):
                splatted_variable_name = unsplatted_variable_name + f"_{i}"
                splatted_variable_names = splatted_variable_names.append(
                    splatted_variable_name
                )
                task.code += f"{splatted_variable_name}, "
            task.code += "]\n"
        else:
            splatted_variable_names = splatted_variable_names.append(
                unsplatted_variable_name
            )
            task.code += f"$unsplatted_variable_name\n"
    task.code += code
    task.value_names = list(
        map(
            lambda x: (x[0].value_id, x[1]),
            zip(splatted_futures, splatted_variable_names),
        )
    )

    # Set `mutated` field of the `Future`s that have been mutated. This is
    # to ensure that future calls to `evaluate` on those `Future`s with
    # `mutated=true` and _only_ those `Future`s will result in an actual
    # evaluation
    for fut in splatted_futures:
        is_task_mutated = False
        for m in task.mutation.values():
            if fut.value_id == m.value_id:
                is_task_mutated = True
        task.effects[fut.value_id] = "MUT" if is_task_mutated else "CONST"


def reassign_futures(
    unsplatted_futures: List[Union[Future, List[Future]]],
    variables: List[Union[Any, list]],
):
    uf = []
    for i in range(len(unsplatted_futures)):
        variable = variables[i]
        if isinstance(unsplatted_futures[i], list):
            uf = unsplatted_futures[i]
            for j in range(len(uf)):
                fe = uf[j]
                set_sample(fe, variable[j])
        else:
            uf = [unsplatted_futures[i]]
            set_sample(uf[0], variable)


@dispatch
def get_samples(uf: Future):
    return sample(uf)


@dispatch
def get_samples(ufs: List[Future]):
    return list(map(sample, ufs))


def partitioned_code_region(
    variables: list,  # TODO: List[Expr]
    variable_names: List[str],
    code_string,
    f,
):

    # What are splatted futures? This is basically the concept that you might
    # have a variable-length list of futures that you need to pass into an
    # annotated code region. We handle this by alowing you to pass in a `Vector{Future}`
    # in the "arguments" of @partitioned. We will then splat the futures into an array
    # with variable names generated according to the index of the future in
    # the list of futures they came from.

    # Convert arguments to `Future`s if they aren't already. Then splat them
    flatten = lambda *n: (
        e for a in n for e in (flatten(*a) if isinstance(a, (tuple, list)) else (a,))
    )
    unsplatted_futures = list(
        map(
            lambda v: [Future(fut) for fut in v]
            if (isinstance(v, list) or isinstance(v, tuple))
            else Future(v),
            variables,
        )
    )
    splatted_futures = get_splatted_futures(unsplatted_futures)

    # Construct assigning_samples (skip for now)
    assigning_samples = [
        get_samples(unsplatted_futures[i]) for i in range(len(variables))
    ]

    prepare_task_for_partitioned_code_region(
        unsplatted_futures, variable_names, splatted_futures, code_string
    )

    try:
        f(*assigning_samples)
        reassign_futures(unsplatted_futures, variables)
    except:
        finish_task()
        raise

    finish_partitioned_code_region(splatted_futures)


# Decorator
# Example usage:
#   @partitioned(fut, res)
#   def f(fut, res):
#       res = copy(fut)
def partitioned(*args):
    # Load in variables
    variables = args
    variable_names = [
        var_name
        for var_name, var_val in inspect.currentframe().f_back.f_locals.items()
        if var_val in args
    ]

    def inner(f):
        # Load code as string
        i = inspect.getsource(f).index(":\n")
        code = dedent(inspect.getsource(f)[i + 1 :])

        # Wrap function so that it returns arguments
        def f_(*args_):
            f(*args_)
            return args_

        res = partitioned_code_region(
            variables,
            variable_names,
            code,
            f_,
        )

        return f_

    return inner


############################################################################
# Helper functions for compiling PAs to send as part of tasks in `compute` #
############################################################################


def duplicate_arg(
    arg: PartitionTypeReference, pa: PartitionAnnotation
) -> PartitionTypeReference:
    v, idx = arg
    return (v, idx + (len(pa.partitions.pt_stacks[v].pts) // 2))


def duplicate_args(
    args: List[PartitionTypeReference],
    pa: PartitionAnnotation,
) -> List[PartitionTypeReference]:
    return list(map(lambda arg: duplicate_arg(arg, pa), args))


def apply_default_constraints(pa: PartitionAnnotation):
    # Add Cross constraints for all unconstrained PTs
    for (v, pt_stack) in pa.partitions.pt_stacks.items():
        for i in range(len(pt_stack.pts)):
            # Check if in_cross_or_co
            in_cross_or_co = False
            for c in pa.constraints.constraints:
                if ((c.type == "CROSS") or (c.type == "CO")) and ((v, i - 1) in c.args):
                    in_cross_or_co = True
                elif (c.type == "CO_GROUP") and any(
                    [(v, i - 1) in group for group in c.args]
                ):
                    in_cross_or_co = True

            # Add Cross constraint for those not constrained in any way
            if not in_cross_or_co:
                pa.constraints.constraints = pa.constraints.constraints.append(
                    PartitioningConstraintOverGroup(
                        "CROSS", PartitionTypeReference[(v, i - 1)]
                    ),
                )

    # Add Co constraint for all Cross-ed PTs

    # Find all co-partitioned PTs
    # inv: Every PT has been Cross-ed
    co_args = []
    co_group_args = []
    for c in pa.constraints.constraints:
        if c.type == "CROSS":
            if len(c.args) == 1:
                co_args = co_args.append(c.args[0])
            elif len(c.args) > 1:
                co_group_args = co_group_args.append(deepcopy(c.args))
    if (len(co_group_args) == 1) and (len(co_args) > 0):
        co_group_args = co_group_args.append(co_args[0:1])

    # Add constraints
    if len(co_args) > 0:
        pa.constraints.constraints = pa.constraints.constraints.append(
            PartitioningConstraintOverGroup("CO", co_args)
        )
    if len(co_group_args) > 0:
        pa.constraints.constraints = pa.constraints.constraints.append(
            PartitioningConstraintOverGroups("CO_GROUP", co_group_args),
        )
    # TODO: Add MemoryUsage constraint by default which computes sample size or
    # defaults to 0


def duplicated_constraints_for_batching(
    pc: PartitioningConstraints, pa: PartitionAnnotation
) -> PartitioningConstraints:
    new_pcs = []
    for c in pc.constraints:
        c_type = c.type
        if (
            (c_type == "CO")
            or (c_type == "EQUAL")
            or (c_type == "SEQUENTIAL")
            or (c_type == "MATCH")
            or c.type.startswith("MATCH_ON=")
        ):
            new_pcs = new_pcs.append(deepcopy(c))
            new_pcs = new_pcs.append(
                PartitioningConstraintOverGroup(c_type, duplicate_args(c.args, pa))
            )
        elif (c_type == "CROSS") or (c_type.startswith("AT_MOST=")):
            # Note that with Cross constraints, the order of the
            # arguments matters. But actually that doesnt matter.
            # The scheduler will automaticcally ensure that the order
            # of PTs in a PT stack is obeyed.
            # ||
            # c.type == "MATCH" || startswith(c.type, "MATCH_ON")
            new_c_args = deepcopy(c.args)
            new_c_args = new_c_args.append(duplicate_args(c.args, pa))
            new_pcs = new_pcs.append(
                PartitioningConstraintOverGroup(
                    c_type,
                    new_c_args,
                )
            )
        elif c_type == "CO_GROUP":
            new_co_args = []
            for group in c.co_args:
                new_co_args = new_co_args.append(duplicate_args(group, pa))
            new_pcs = new_pcs.append(
                PartitioningConstraintOverGroups(c_type, new_co_args),
            )
        elif c_type.startswith("SCALE_BY="):
            # `ScaleBy` constraints are not duplicated. They must refer to
            # only the first PT of the PT compositions they reference.
            new_pcs = new_pcs.append(c)
    return PartitioningConstraints(new_pcs)


def duplicate_for_batching(pa: PartitionAnnotation):
    # Duplicate PT stacks
    for pt_stack in pa.partitions.pt_stacks.values():
        # Copy over the PT stack
        second_half = deepcopy(pt_stack.pts)

        # Append to form a compositions of PTs that is twic in length
        pt_stack.pts.extend(second_half)

    # Duplicate annotation-level constraints for Co, Equal, Cross, AtMost, ScaleBy
    pa.constraints = duplicated_constraints_for_batching(pa.constraints, pa)

    # Add constraints for second half being Sequential and Match-ing the first
    # half
    for (v, pt_stack) in pa.partitions.pt_stacks.items():
        for i in range(len(pt_stack.pts) // 2):
            # TODO: Is this indexing correct? Should we subtract 1?
            dupi = i + (len(pt_stack.pts) // 2)

            # Add in `Sequential` and `Match` constraints for the duplicated
            # part of the PT composition
            pa.constraints.constraints = pa.constraints.constraints.append(
                PartitioningConstraintOverGroup("SEQUENTIAL", [(v, dupi - 1)]),
            )
            # Since we have duplicated all constraints, we don't need to
            # constrain both halves of the PT composition to match

            # Duplicate PT-level constraints
            pt_dup = pt_stack.pts[dupi]
            pt_stack.pts[dupi].constraints = duplicated_constraints_for_batching(
                pt_dup.constraints, pa
            )
