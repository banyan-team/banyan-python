from typing import Any, Dict

from plum import dispatch

from sample import Sample

LocationParameters = Dict[str, Any]


class Location:
    @dispatch
    def __init__(
        self,
        src_name: str,
        dst_name: str,
        src_parameters: LocationParameters,
        dst_parameters: LocationParameters,
        total_memory_usage: int,
        sample: Sample,
        parameters_invalid: bool,
        sample_invalid: bool,
    ):

        self.src_name = src_name
        self.dst_name = dst_name
        self.src_parameters = src_parameters
        self.dst_parameters = dst_parameters
        self.total_memory_usage = total_memory_usage
        self.sample = sample
        self.parameters_invalid = parameters_invalid
        self.sample_invalid = sample_invalid

    @dispatch
    def __init__(
        self,
        name: str,
        parameters: LocationParameters,
        total_memory_usage: int = -1,
        sample: Sample = Sample(),
    ):
        self.__init__(
            name, name, parameters, parameters, total_memory_usage, sample, False, False
        )

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
            "total_memory_usage": (
                None if (self.total_memory_usage == -1) else self.total_memory_usage,
            ),
        }
