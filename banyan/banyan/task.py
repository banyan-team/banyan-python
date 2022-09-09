from typing import Callable, Dict, List, Tuple

from plum import dispatch

from .future import Future
from .id import ValueId
from .partitions import NOTHING_PARTITIONED_USING_FUNC
from .utils_partitions import (
    PartitionAnnotation,
    PartitionedUsingFunc,
    PartitioningConstraint,
)


class DelayedTask:
    """Stores information about a task/code region to be run in the future"""

    @dispatch
    def __init__(
        self,
        used_modules: List[str],
        code: str,
        value_names: List[Tuple[ValueId, str]],
        effects: Dict[ValueId, str],
        pa_union: List[PartitionAnnotation],
        memory_usage: Dict[ValueId, Dict[str, int]],
        partitioned_using_func: PartitionedUsingFunc,
        partitioned_with_func: Callable,
        futures: List[Future],
        mutation: Dict[Future, Future],
        inputs: List[Future],
        outputs: List[Future],
        scaled: List[Future],
        keep_same_sample_rate: bool,
        memory_usage_constraints: List[PartitioningConstraint],
        additional_memory_usage_constraints: List[PartitioningConstraint],
        input_value_ids: List[ValueId],
        output_value_ids: List[ValueId],
        scaled_value_ids: List[ValueId],
    ):

        self.used_modules = used_modules
        self.code = code
        self.value_names = value_names
        self.effects = effects
        self.pa_union = pa_union
        self.memory_usage = memory_usage
        self.partitioned_using_func = partitioned_using_func
        self.partitioned_with_func = partitioned_with_func
        self.futures = futures
        self.mutation = mutation
        self.inputs = inputs
        self.outputs = outputs
        self.scaled = scaled
        self.keep_same_sample_rate = keep_same_sample_rate
        self.memory_usage_constraints = memory_usage_constraints
        self.additional_memory_usage_constraints = additional_memory_usage_constraints
        self.input_value_ids = input_value_ids
        self.output_value_ids = output_value_ids
        self.scaled_value_ids = scaled_value_ids

    @dispatch
    def __init__(self):
        self.__init__(
            [],
            "",
            [],
            {},
            [PartitionAnnotation()],
            {},
            NOTHING_PARTITIONED_USING_FUNC,
            lambda x: x,
            [],
            {},
            [],
            [],
            [],
            True,
            [],
            [],
            [],
            [],
            [],
        )

    def to_py(self):
        return {
            "code": self.code,
            "value_name": self.value_names,
            "effects": self.task.effects,
            "pa_union": [pa.to_py() for pa in self.pa_union],
            "memory_usage": self.memory_usage,
            "inputs": self.input_value_ids,
            "outputs": self.output_value_ids,
            "keep_same_sample_rate": self.keep_same_sample_rate,
        }
