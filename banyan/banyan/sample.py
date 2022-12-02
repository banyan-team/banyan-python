from typing import Any, List

from plum import dispatch

from .location import get_sample_rate
from .session import get_session
from .utils import sample_memory_usage


class Sample:
    @dispatch
    def __init__(
        self,
        value: Any,
        object_id: int,
        memory_usage: int,
        rate: int,
        grouping_keys: List[Any],
    ):
        self.value = value
        self.object_id = object_id
        self.memory_usage = memory_usage
        self.rate = rate
        self.grouping_keys = grouping_keys

    @dispatch
    def __init__(self):
        self.__init__(None, id(None), 0, get_sample_rate(), [])

    @dispatch
    def __init__(self, value: Any, sample_memory_usage: int, sample_rate: int):
        memory_usage = int(round(sample_memory_usage / sample_rate))
        self.__init__(value, id(value, memory_usage, sample_rate, []))

    @dispatch
    def __init__(self, value: Any, sample_rate: int):
        self.__init__(value, id(value), sample_memory_usage(value), sample_rate, [])

    def is_none(self):
        return self.rate == -1


NOTHING_SAMPLE = Sample(None, -1)


class SamplingConfig:
    def __init__(
        self,
        rate: int,
        always_exact: bool,
        max_num_bytes_exact: int,
        force_new_sample_rate: bool,
        assume_shuffled: bool,
    ):
        self.rate = rate
        self.always_exact = always_exact
        self.max_num_bytes_exact = max_num_bytes_exact
        self.force_new_sample_rate = force_new_sample_rate
        self.assume_shuffled = assume_shuffled
