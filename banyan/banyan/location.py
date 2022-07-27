from typing import Any, Dict

LocationParameters = Dict[str,Any]

class Location:
    """??
    """
    def __init__ (
        self,  
        src_name: str,
        dst_name: str, 
        src_parameters: LocationParameters,
        dst_parameters: LocationParameters,
        total_memory_usage: int,
        sample: Sample,
        parameters_invalid: bool,
        sample_invalid: bool    
    ):
        
        self.src_name = src_name
        self.dst_name = dst_name 
        self.src_parameters = src_parameters
        self.dst_parameters = dst_parameters
        self.total_memory_usage = total_memory_usage
        self.sample = sample
        self.parameters_invalid = parameters_invalid
        self.sample_invalid = sample_invalid  


    def is_none(self):
        return l.sample.is_none()