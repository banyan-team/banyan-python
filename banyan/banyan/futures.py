###########
# Futures #
###########

# Every future does have a location assigned to it: both a source and a
# location. If the future is created from some remote location, the
# location will have a memory usage that will tell the future how much memory
# is used.

from copy import deepcopy
from typing import Any

from annotation import mutated
from future import create_future, Future, NOTHING_FUTURE
from id import generate_value_id
from location import Location
from locations import (
    Client,
    destined,
    located,
    Nothing,
    NOTHING_LOCATION,
    sourced,
    Value,
)
from partitions import ValueId
from session import get_session
from utils import total_memory_usage


def get_location(o) -> Location:
    if isinstance(o, ValueId):
        return get_session().locations.get(o, NOTHING_LOCATION)
    elif isinstance(o, Future):
        return get_session().locations.get(o.value_id, NOTHING_LOCATION)


def create_new_future(source: Location, mutate_from: Future, datatype: str):
    # Generate new value id
    value_id: ValueId = generate_value_id()

    # Create new Future and assign a location to it
    new_future = create_future(datatype, None, value_id, False, True)
    sourced(new_future, source)
    destined(new_future, Nothing())

    # TODO: Add Size location here if needed
    # Handle locations that have an associated value
    source_src_name = source.src_name
    if (
        source_src_name == "None"
        or source_src_name == "Client"
        or source_src_name == "Value"
    ):
        new_future.value = source.sample.value
        new_future.stale = False

    if mutate_from is not None:
        # Indicate that this future is the result of an in-place mutation of
        # some other value
        mutated(mutate_from, new_future)
    elif source.src_name == "None":
        # For convenience, if a future is constructed with no location to
        # split from, we assume it will be mutated in the next code region
        # and mark it as mutated. This is pretty common since often when
        # we are creating new futures with None location it is as an
        # intermediate variable to store the result of some code region.
        #
        # Mutation can also be specified manually with mutate=true|false in
        # `partition` or implicitly through `Future` constructors
        mutated(new_future)
    return new_future


def create_future_from_sample(value: Any, datatype: str) -> Future:
    # TODO: Store values in S3 instead so that we can read from there
    location: Location = (
        Value(value) if total_memory_usage(value) <= (4 * 1024) else Client(value)
    )

    # Create future, store value, and return
    return create_new_future(location, NOTHING_FUTURE, datatype)


# Constructs a future from a future that was already created.

# If the given future has not had its value mutated (meaning that the value
# stored with it here on the client is the most up-to-date version of it), we use
# its value to construct a new future from a copy of the value.

# However, if the future has been mutated by some code region that has already
# been recorded, we construct a new future with location `None` and mark it as
# mutated. This is because presumably in the case that we _can't_ copy over the
# given future, we would want to assign to it in the upcoming code region where
# it's going to be used.


def create_future_from_existing(fut: Future, mutation: Any) -> Future:
    if not fut.stale:
        # Copy over value
        new_future = create_future(
            fut.datatype,
            deepcopy(mutation(fut.value)),
            generate_value_id(),
            # If the future is not stale, it is not mutated in a way where
            # a further `compute` is needed. So we can just copy its value.
            False,
            False,
        )

        # Copy over location
        located(new_future, deepcopy(get_location(fut)))

        new_future
    else:
        create_new_future(Nothing(), NOTHING_FUTURE, fut.datatype)


class NothingValue:
    def __init__(self):
        pass


NOTHING_VALUE = NothingValue()


def Future(
    value: Any = NOTHING_VALUE,
    datatype="Any",
    source: Location = Nothing(),
    mutate_from: Future = None,
    from_future: Future = None,
    mutation: Any = (lambda x: x),
):
    if from_future is not None:
        return create_future_from_existing(from_future, mutation)
    elif isinstance(value, NothingValue):
        return create_new_future(
            source, NOTHING_FUTURE if (mutate_from is None) else mutate_from, datatype
        )
    else:
        return create_future_from_sample(value, datatype)
