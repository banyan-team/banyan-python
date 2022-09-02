from copy import deepcopy
from datetime import datetime
import json
import logging
import os
from typing import Any, Callable, Dict, List, Tuple, Union

from annotation import (
    apply_default_constraints,
    apply_mutation,
    apply_partitioned_using_func,
    duplicate_for_batching,
    finish_task,
    mutated,
    partitioned_with,
    pt,
    set_task,
)
from future import destroy_future, Future, isview, value_id_getter
from id import (
    get_num_bang_values_issued,
    ResourceId,
    SessionId,
    set_num_bang_values_issued,
    ValueId,
)
from location import Location
from locations import (
    Client,
    destined,
    Disk,
    get_dst_name,
    Nothing,
    NOTHING_LOCATION,
    sourced,
)
from partitions import NOTHING_PARTITIONED_USING_FUNC
from pfs import forget_parents
from pt_lib_constructors import Replicated
from queues import (
    get_gather_queue,
    get_scatter_queue,
    receive_next_message,
    send_message,
)
from request import DestroyRequest, RecordTaskRequest, Request
from session import Session
from sessions import (
    end_session,
    get_session,
    get_sessions_dict,
    get_session_id,
    wait_for_session,
)
from task import DelayedTask
from utils import (
    from_py_value_contents,
    send_request_get_response,
    get_loaded_packages,
    to_py_value_contents,
)


#################
# Magic Methods #
#################

# TODO: Implement magic methods

# Assume that this is mutating
# function Base.getproperty(fut::Future, sym::Symbol)
# end

# Mutating
# TODO: put this back in some way
# function Base.setproperty!(fut::Future, sym::Symbol, new_value)
# end

##################
# Helper methods #
##################


def check_worker_stuck_error(
    message: Dict[str, Any],
    error_for_main_stuck: str,
    error_for_main_stuck_time: datetime.DateTime,
) -> Tuple[str, datetime.DateTime]:
    value_id = message["value_id"]
    if (value_id == "-2") and (error_for_main_stuck_time is None):
        error_for_main_stuck_msg = from_py_value_contents(message["contents"])
        if f"session {get_session_id()}" in error_for_main_stuck_msg:
            error_for_main_stuck = error_for_main_stuck_msg
            error_for_main_stuck_time = datetime.now()
    return error_for_main_stuck, error_for_main_stuck_time


def check_worker_stuck(
    error_for_main_stuck: str, error_for_main_stuck_time: datetime.DateTime
) -> str:
    if (
        (error_for_main_stuck is not None)
        and (error_for_main_stuck_time is not None)
        and ((datetime.now() - error_for_main_stuck_time).seconds > 10)
    ):
        print(error_for_main_stuck)
        print(
            "The above error occurred on some workers but other workers are still running. "
            "Please interrupt and end the session unless you expect that a lot of logs are being returned."
        )
        error_for_main_stuck = None
    return error_for_main_stuck


#############################
# Basic methods for futures #
#############################

destroyed_value_ids: List[ValueId] = []


def get_destroyed_value_ids():
    global destroyed_value_ids
    return destroyed_value_ids


def _partitioned_computation_concrete(
    fut: Future,
    destination: Location,
    new_source: Location,
    sessions: Dict[SessionId, Session],
    session_id: SessionId,
    session: Session,
    resource_id: ResourceId,
    destroyed_value_ids: List[ValueId],
):
    # TODO: @partitioned fut begin end

    # Get all tasks to be recorded in this call to `compute`
    tasks = [
        req.task
        for req in session.pending_requests
        if isinstance(req, RecordTaskRequest)
    ]
    tasks_reverse = deepcopy(tasks)
    tasks_reverse.reverse()

    # Call `partitioned_using_func`s in 2 passes - forwards and backwards.
    # This allows sample properties to propagate in both directions. We
    # must also make sure to apply mutations in each task appropriately.
    for t in tasks_reverse:
        apply_mutation(t.mutation, True)

    for t in tasks:
        set_task(t)
        if t.partitioned_using_func is not None:
            apply_partitioned_using_func(t.partitioned_using_func)

        apply_mutation(t.mutation, False)

    for t in tasks_reverse:
        set_task(t)
        apply_mutation(t.mutation, True)
        if t.partitioned_using_func is not None:
            apply_partitioned_using_func(t.partitioned_using_func)

    # Do further processing on tasks now that all samples have been
    # computed and sample properties have been set up to share references
    # as needed to prevent expensive redundant computation of sample
    # properties like divisions
    for t in tasks:
        apply_mutation(t.mutation, False)

        # Call `partitioned_with_func` to create additional PAs for each task
        set_task(t)
        if t.partitioned_with_func is not (
            lambda x: x
        ):  # TODO: Ensure that this will work
            partitioned_with_func: Any = t.partitioned_with_func
            partitioned_with_func(t.futures)

        # Cascade PAs backwards. In other words, if as we go from first to
        # last PA we come across one that's annotating a value not
        # annotated in a previous PA, we copy over the annotation (the
        # assigned PT stack) to the previous PA.
        for (j, pa) in enumerate(t.pa_union):
            # For each PA in this PA union for this task, we consider the
            # PAs before it
            pa_union_reverse = deepcopy(t.pa_union[1 : j - 1])
            pa_union_reverse.reverse()
            for previous_pa in pa_union_reverse:
                for value_id in pa.partitions.pt_stacks.keys():
                    # Check if there is a previous PA where this value
                    # does not have a PT.
                    if value_id not in previous_pa.partitions.pt_stacks:
                        # Cascade the PT composition backwards
                        previous_pa.partitions.pt_stacks[value_id] = deepcopy(
                            pa.partitions.pt_stacks[value_id]
                        )

                        # Cascade backwards all constraints that mention the
                        # value. NOTE: If this is not desired, users should
                        # be explicit and assign different PT compositions for
                        # different values.
                        for constraint in pa.constraints.constraints:
                            # Determine whether we should copy over this constraint
                            copy_constraint = False
                            if len(constraint.args) != 0:
                                for arg in constraint.args:
                                    arg_v = arg[0]
                                    if arg_v == value_id:
                                        copy_constraint = True
                            elif len(constraint.co_args) != 0:
                                for arg in constraint.co_args:
                                    for subarg in arg:
                                        subarg_v = subarg[0]
                                        if subarg_v == value_id:
                                            copy_constraint = True

                            # Copy over constraint
                            if copy_constraint:
                                previous_pa.constraints.constraints.append(
                                    deepcopy(constraint)
                                )

    # Switch back to a new task for next code region
    finish_task()

    # Iterate through tasks for further processing before recording them
    forget_parents()
    for t in tasks:
        # Apply defaults to PAs
        for pa in t.pa_union:
            apply_default_constraints(pa)
            duplicate_for_batching(pa)

        # Destroy all closures so that all references to `Future`s are dropped
        t.partitioned_using_func = NOTHING_PARTITIONED_USING_FUNC
        t.partitioned_with_func = lambda x: x
        t.futures = []

        # Handle
        t.mutation = {}  # Drop references to `Future`s here as well

        t.input_value_ids = map(value_id_getter, t.inputs)
        t.output_value_ids = map(value_id_getter, t.outputs)
        t.scaled_value_ids = map(value_id_getter, t.scaled)
        t.inputs = []
        t.outputs = []
        t.scaled = []

    # Finalize (destroy) all `Future`s that can be destroyed
    GC.gc()

    # Destroy everything that is to be destroyed in this task
    for req in session.pending_requests:
        # Don't destroy stuff where a `DestroyRequest` was produced just
        # because of a `mutated(old, new)`
        if isinstance(req, DestroyRequest):
            req_value_id = req.value_id
            destroyed_value_ids.append(req_value_id)
            # If this value was to be downloaded to or uploaded from the
            # client side, delete the reference to its data. We do the
            # `GC.gc()` before this and store `futures_on_client` in a
            # `WeakKeyDict` just so that we can ensure that we can actually
            # garbage-collect a value if it's done and only keep it around if
            # a later call to `collect` it may happen and in that call to
            # `collect` we will be using `partitioned_computation` to
            # communicate with the executor and fill in the value for that
            # future as needed and then return `the_future.value`.
            if req_value_id in session.futures_on_client:
                session.futures_on_client.delete(req_value_id)

            # Remove information about the value's location including the
            # sample taken from it
            session.locations.delete(req_value_id)
    # end of preparing tasks

    # Send evaluation request
    is_merged_to_disk: bool = False
    try:
        response = send_evaluation(fut.value_id, session_id)
        is_merged_to_disk = response["is_merged_to_disk"]
    except:
        end_session(failed=True)
        raise  # TODO: Double check this to rethrow exception

    # Get queues for moving data between client and cluster
    scatter_queue = get_scatter_queue()
    gather_queue = get_gather_queue()

    # There are two cases: either we
    # TODO: Maybe we don't need to wait_For_session

    # There is a problem where we start a session with nowait=true and then it
    # reuses a resource that is in a creating state. Since the session is still
    # creating and we have not yet waited for it to start, if we have
    # `estimate_available_memory=false` then we will end up with job info not
    # knowing the available memory and unable to schedule. We definitely should
    # ensure that no resource is ending up in a creating state when it has
    # been clearly destroyed and that is a problem to fix with destroy-sessions.
    # However, there are some options to address this:
    # - wait_for_session before calling get_session_status
    # - always estimate_available_memory if we do nowait
    # - always only wait for session at the start since if you're loading data
    # then you will probably do sample collection first and need to wait for
    # session anyway
    # We'll go with the last one and understand that you will pay the price of
    # loading packages twice. This is bad but even if you created the session
    # without waiting, you would still have to suffer from intiial compilation
    # time for functions on the client side and on the executor and that can't
    # be overlapped. The best we can do is try to create sysimages for Banyan*
    # libraries to reduce the package loading time on the executor.

    # Read instructions from gather queue
    # @debug "Waiting on running session $session_id, listening on $gather_queue, and computing value with ID $(fut.value_id)"
    error_for_main_stuck = None
    error_for_main_stuck_time = None
    partial_gathers = {}
    while True:
        # TODO: Use to_jl_value and from_jl_value to support Client
        message, error_for_main_stuck = receive_next_message(
            gather_queue, error_for_main_stuck, error_for_main_stuck_time
        )
        message_type = message["kind"]
        if message_type == "SCATTER_REQUEST":
            # Send scatter
            value_id = message["value_id"]
            if value_id not in session.futures_on_client:
                raise ("Expected future to be stored on client side")
            f = session.futures_on_client[value_id]
            # @debug "Received scatter request for value with ID $value_id and value $(f.value) with location $(get_location(f))"
            send_message(
                scatter_queue,
                json.dumps(
                    {"value_id": value_id, "contents": to_py_value_contents(f.value)},
                ),
            )
            # TODO: Update stale/mutated here to avoid costly
            # call to `send_evaluation`
        elif message_type == "GATHER":
            # Receive gather
            value_id: ValueId = message["value_id"]
            if value_id not in partial_gathers:
                partial_gathers[value_id]: str = message["contents"]
            else:
                partial_gathers[value_id] *= message["contents"]

        elif message_type == "GATHER_END":  # NOTE: CONTINUE FROM HERE
            value_id = message["value_id"]
            contents = partial_gathers.get(value_id, "") * message["contents"]
            # @debug "Received gather request for $value_id"
            if value_id in session.futures_on_client:
                value = from_py_value_contents(contents)
                f = session.futures_on_client[value_id]
                f.value = value
                # TODO: Update stale/mutated here to avoid costly
                # call to `send_evaluation`
            error_for_main_stuck, error_for_main_stuck_time = check_worker_stuck_error(
                message, error_for_main_stuck, error_for_main_stuck_time
            )
        elif message_type == "EVALUATION_END":
            if message["end"]:
                break

    # Update `mutated` and `stale` for the future that is being evaluated
    fut.mutated = False
    # TODO: See if there are more cases where you a `compute` call on a future
    # makes it no longer stale
    if get_dst_name(fut) == "Client":
        fut.stale = False

    # This is where we update the location source.
    use_new_source_func = False
    if is_merged_to_disk:
        sourced(fut, Disk())
    else:
        if new_source is not None:
            sourced(fut, new_source)
        else:
            use_new_source_func = True

    # Reset the location destination to its default. This is where we
    # update the location destination.
    destined(fut, Nothing())

    use_new_source_func


def partitioned_computation_concrete(
    handler: Callable,
    fut: Future,
    destination: Location,
    new_source: Location,
    new_source_func: Callable,
):

    # NOTE: Right now, `compute` wil generally spill to disk (or write to some
    # remote location or download to the client). It will not just persist data
    # in memory. In Spark or Dask, it is recommended that you call persist or
    # compute or something like that in order to cache data in memory and then
    # ensure it stays there as you do logistic regression or some iterative
    # computation like that. With an iterative computation like logistic
    # regression in Banyan, you would only call `compute` on the result and we
    # would be using a same future for each iteration. Each iteration would
    # correspond to a separate stage with casting happening between them. And
    # the scheduler would try as hard as possible to keep the whole thing in
    # memory. This is because unlike Dask, we allow a Future to be reused
    # across tasks. If we need `compute` to only evaluate and persist in-memory
    # we should modify the way we schedule the final merging stage to not
    # require the last value to be merged simply because it is being evaluated.

    sessions = get_sessions_dict()
    session_id = get_session_id()
    session = get_session()
    resource_id = session.resource_id

    destroyed_value_ids = get_destroyed_value_ids()
    if fut.value_id in destroyed_value_ids:
        raise Exception("Cannot compute a destroyed future")

    destination_dst_name = destination.dst_name
    if (
        fut.mutated
        or ((destination_dst_name == "Client") and fut.stale)
        or (destination_dst_name == "Remote")
    ):
        # TODO: Check to ensure that `fut` is annotated
        # This creates an empty final task that ensures that the future
        # will be scheduled to get sent to its destination.
        destined(fut, destination)
        mutated(fut)
        partitioned_with(handler, [fut], scaled=[fut])

        use_new_source_func = _partitioned_computation_concrete(
            fut,
            destination,
            new_source,
            sessions,
            session_id,
            session,
            resource_id,
            destroyed_value_ids,
        )
        # TODO: If not still merged to disk, we need to lazily set the location source to something else
        if use_new_source_func:
            if new_source_func is not (lambda x: x):
                sourced(fut, new_source_func)
            else:
                # TODO: Maybe suppress this warning because while it may be
                # useful for large datasets, it is going to come up for
                # every aggregateion result value that doesn't have a source
                # but is being computed with the Client as its location.
                if destination.src_name == "None":
                    # It is not guaranteed that this data can be used again.
                    # In fact, this data - or rather, this value - can only be
                    # used again if it is in memory. But because it is up to
                    # the schedule to determine whether it is possible for the
                    # data to fit in memory, we can't be sure that it will be
                    # in memory. So this data should have first been written to
                    # disk with `compute_inplace` and then only written to this
                    # unreadable location.
                    logging.warning(
                        f"Value with ID {fut.value_id} has been written to a location that "
                        "cannot be used as a source and it is not on disk. Please do not "
                        "attempt to use this value again. If you wish to use it again, please "
                        "write it to disk with `compute_inplace` before writing it to a location."
                    )
                sourced(fut, destination)

    # Reset the annotation for this partitioned computation
    set_task(DelayedTask())

    # NOTE: One potential room for optimization is around the fact that
    # whenever we compute something we fully merge it. In fully merging it,
    # we spill it out of memory. Maybe it might be kept in memory and we don't
    # need to set the new source of something being `collect`ed to `Client`.

    return fut


def partitioned_computation(
    handler: Callable,
    fut: Future,
    destination: Location,
    new_source: Union[Callable, Location] = NOTHING_LOCATION,
):
    if isview(fut):
        raise Exception(
            "Computing a view (such as a GroupedDataFrame) is not currently supported"
        )
    if isinstance(new_source, Callable):
        new_source_func = new_source
        new_source = NOTHING_LOCATION
    else:
        new_source_func = lambda x: x
    partitioned_computation_concrete(
        handler, fut, destination, new_source, new_source_func
    )


# Scheduling options
report_schedule = False
encourage_parallelism = False
encourage_parallelism_with_batches = False
exaggurate_size = False
encourage_batched_inner_loop = False
optimize_cpu_cache = False


def get_report_schedule() -> bool:
    global report_schedule
    return report_schedule


def get_encourage_parallelism() -> bool:
    global encourage_parallelism
    return encourage_parallelism


def get_encourage_parallelism_with_batches() -> bool:
    global encourage_parallelism_with_batches
    return encourage_parallelism_with_batches


def get_exaggurate_size() -> bool:
    global exaggurate_size
    return exaggurate_size


def get_encourage_batched_inner_loop() -> bool:
    global encourage_batched_inner_loop
    return encourage_batched_inner_loop


def get_optimize_cpu_cache() -> bool:
    global optimize_cpu_cache
    return optimize_cpu_cache


def configure_scheduling(**kwargs):
    global report_schedule
    global encourage_parallelism
    global encourage_parallelism_with_batches
    global exaggurate_size
    global encourage_batched_inner_loop
    global optimize_cpu_cache
    kwargs_name = kwargs.get("name", "")
    report_schedule = kwargs.get("report_schedule", False) or ("name" in kwargs)
    if kwargs.get("encourage_parallelism", False) or (
        kwargs_name == "parallelism encouraged"
    ):
        encourage_parallelism = True
    if ("encourage_parallelism_with_batches" in kwargs) or (
        kwargs_name == "parallelism and batches encouraged"
    ):
        encourage_parallelism = True
        encourage_parallelism_with_batches = True
    if kwargs.get("exaggurate_size", False) or (kwargs_name == "size exaggurated"):
        exaggurate_size = True
    if kwargs.get("encourage_batched_inner_loop", False) or (
        kwargs_name == "parallelism and batches encouraged"
    ):
        encourage_batched_inner_loop = True
    if "optimize_cpu_cache" in kwargs:
        optimize_cpu_cache = kwargs["optimize_cpu_cache"]
    if kwargs_name == "default scheduling":
        encourage_parallelism = False
        encourage_parallelism_with_batches = False
        exaggurate_size = False
        encourage_batched_inner_loop = False
        optimize_cpu_cache = False


def send_evaluation(value_id: ValueId, session_id: SessionId):
    # First we ensure that the session is ready. This way, we can get a good
    # estimate of available worker memory before calling evaluate.
    wait_for_session(session_id)

    encourage_parallelism = get_encourage_parallelism()
    encourage_parallelism_with_batches = get_encourage_parallelism_with_batches()
    exaggurate_size = get_exaggurate_size()
    encourage_batched_inner_loop = get_encourage_batched_inner_loop()
    optimize_cpu_cache = get_optimize_cpu_cache()

    # Note that we do not need to check if the session is running here, because
    # `evaluate` will check if the session has failed. If the session is still creating,
    # we will proceed with the eval request, but the client side will wait
    # for the session to be ready when reading from the queue.

    logging.debug("Sending evaluation request")

    # Get list of the modules used in the code regions here
    record_task_requests = filter(
        lambda req: isinstance(req, RecordTaskRequest), session.pending_requests
    )
    used_packages = []
    for record_task_request in record_task_requests:
        for used_module in record_task_request.task.used_modules:
            used_packages.append(used_module)
    if len(used_packages) == 0:
        used_packages = set()
    else:
        used_packages = set.union(*used_packages)  # remove duplicates

    # Submit evaluation request
    session = get_session()
    if (session.organization_id is None) or (session.organization_id == ""):
        raise Exception("Organization ID not stored locally for this session")
    if (session.cluster_instance_id is None) or (session.cluster_instance_id == ""):
        raise Exception("Cluster instance ID not stored locally for this session")
    not_using_modules = session.not_using_modules
    if not not_using_modules:
        raise Exception(
            "Modules not to be used are not stored locally for this session"
        )
    main_modules = get_loaded_packages().difference(not_using_modules)
    using_modules = used_packages.difference(not_using_modules)
    response = send_request_get_response(
        "evaluate",
        {
            "value_id": value_id,
            "session_id": session_id,
            "requests": [r.to_py() for r in get_session().pending_requests],
            "options": {
                "report_schedule": report_schedule,
                "encourage_parallelism": encourage_parallelism,
                "encourage_parallelism_with_batches": encourage_parallelism_with_batches,
                "exaggurate_size": exaggurate_size,
                "encourage_batched_inner_loop": encourage_batched_inner_loop,
                "optimize_cpu_cache": optimize_cpu_cache,
            },
            "num_bang_values_issued": get_num_bang_values_issued(),
            "main_modules": main_modules,
            "partitioned_using_modules": using_modules,
            "benchmark": os.getenv("BANYAN_BENCHMARK", default="0") == "1",
            "worker_memory_used": get_session().worker_memory_used,
            "resource_id": get_session().resource_id,
            "organization_id": get_session().organization_id,
            "cluster_instance_id": get_session().cluster_instance_id,
            "cluster_name": get_session().cluster_name,
        },
    )
    if response is None:
        raise Exception("The evaluation request has failed. Please contact support")

    # Update counters for generating unique values
    set_num_bang_values_issued(response["num_bang_values_issued"])

    # Clear global state and return response
    get_session().pending_requests = []
    return response


def pt_for_replicated(futures: List[Future]):
    return pt(futures[0], Replicated())


def compute(f: Future, destroy: List[Future] = []):
    # NOTE: We might be in the middle of an annotation when this is called so
    # we need to avoid partitioned computation (which will reset the task)
    for f_to_destroy in destroy:
        if f_to_destroy.value_id == f.value_id:
            raise Exception("Cannot destroy the future being computed")
        destroy_future(f_to_destroy)

    # Fast case for where the future has not been mutated and isn't stale
    if f.mutated or f.stale:
        # We don't need to specify a `source_after` since it should just be
        # `Client()` and the sample won't change at all. Also, we should already
        # have a sample since we are merging it to the client.

        partitioned_computation(
            pt_for_replicated, f, destination=Client(), new_source=Client(None)
        )

        # NOTE: We can't use `new_source=fut->Client(fut.value)` because
        # `new_source` is for locations that require expensive sample collection
        # and so we would only want to compute that location if we really need to
        # use it as a source. Instead in this case, we really just know initially
        # that this is a destination with _some_ value (so we default it to `nothing`)
        # and then right after when we have actually computed, we will set it to the right
        # location using the computed `fut.value`.
        sourced(f, Client(f.value))

    return f.value


def compute_inplace(fut: Future):
    return partitioned_computation(pt_for_replicated, fut, destination=Disk())


# Make the `offloaded` function on the client side keep looping and
#     (1) checking receive_next_message and
#     (2) checking for message[“kind”] == "GATHER" and
#     (3) `break`ing and `return`ing the value (using `from_jl_value_contents(message["contents"])`)
#         if value_id == -1
# Make `offloaded` function in Banyan.jl
#   which calls evaluate passing in a string of bytes
#   by serializing the given function (just call to_jl_value_contents on it)
#   and passing it in with the parameter offloaded_function_code
#
# Make `offloaded` function specify
#     job_id, num_bang_values_issued, main_modules, and benchmark
#     when calling evaluate (see send_evaluate) and value_id -1
# offloaded(some_func; distributed=true)
# offloaded(some_func, a, b; distributed=true)
def offloaded(given_function: Callable, *args, distributed: bool = False):

    # NOTE: no need for wait_for_session here because evaluate for offloaded
    # doesn't need information about memory usage from intiial package loading.

    # Get serialized function
    serialized = to_py_value_contents((given_function, args))

    # Submit evaluation request
    session = get_session()
    if (session.organization_id is None) or (session.organization_id == ""):
        raise Exception("Organization ID not stored locally for this session")
    if (session.cluster_instance_id is None) or (session.cluster_instance_id == ""):
        raise Exception("Cluster instance ID not stored locally for this session")
    not_using_modules = session.not_using_modules
    main_modules = [m for m in get_loaded_packages() if (m not in not_using_modules)]
    session_id = get_session_id()
    response = send_request_get_response(
        "evaluate",
        {
            "value_id": -1,
            "session_id": session_id,
            "options": {},
            "num_bang_values_issued": get_num_bang_values_issued(),
            "main_modules": main_modules,
            "requests": [],
            "partitioned_using_modules": [],
            "benchmark": os.getenv("BANYAN_BENCHMARK", default="0") == "1",
            "offloaded_function_code": serialized,
            "distributed": distributed,
            "worker_memory_used": get_session().worker_memory_used,
            "resource_id": get_session().resource_id,
            "organization_id": get_session().organization_id,
            "cluster_instance_id": get_session().cluster_instance_id,
            "cluster_name": get_session().cluster_name,
        },
    )
    if response is None:
        raise Exception("The evaluation request has failed. Please contact support")

    # We must wait for session because otherwise we will slurp up the session
    # ready message on the gather queue.
    wait_for_session(session_id)

    # job_id = Banyan.get_job_id()
    # p = ProgressUnknown("Running offloaded code", spinner=true)

    session = get_session()
    gather_queue = get_gather_queue()
    stored_message = None
    error_for_main_stuck, error_for_main_stuck_time = None, None
    partial_gathers = {}
    while True:
        message, error_for_main_stuck = receive_next_message(
            gather_queue, error_for_main_stuck, error_for_main_stuck_time
        )
        message_type = message["kind"]
        if message_type == "GATHER":
            # Receive gather
            value_id = message["value_id"]
            contents = message["contents"]
            if partial_gathers in value_id:
                partial_gathers[value_id] = contents
            else:
                partial_gathers[value_id] += contents
        elif message_type == "GATHER_END":
            value_id = message["value_id"]
            contents = partial_gathers.get(value_id, "") + message["contents"]
            if value_id == "-1":
                memory_used = message["worker_memory_used"]
                # Note that while the memory usage from offloaded computation does get
                # reset with each session even if it reuses the same job, we do
                # recompute the initial available memory every time we start a session
                # and this should presumably include the offloaded memory usage.
                get_session().worker_memory_used = (
                    get_session().worker_memory_used + memory_used
                )
                stored_message = from_py_value_contents(contents)
            error_for_main_stuck, error_for_main_stuck_time = check_worker_stuck_error(
                message, error_for_main_stuck, error_for_main_stuck_time
            )
        elif message_type == "EVALUATION_END":
            if message["end"]:
                return stored_message


###############################################################
# Other requests to be sent with request to evaluate a Future #
###############################################################


def record_request(request: Request):
    get_session().pending_requests.append(request)
