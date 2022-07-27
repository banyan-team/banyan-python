from click import UsageError


class DelayedTask:
    """Stores information about a task/code region to be run int eh future 
    """
    def __init__(
        self, 
        used_modules, 
        code, 
        value_names, 
        effects, 
        pa_union, 
        memory_usage, 
        partitioned_using_func, 
        partitioned_with_func, 
        futures, 
        mutation, 
        inputs, 
        outputs, 
        scaled, 
        keep_same_sample_rate, 
        memory_usage_constraints,
        additional_memory_usage_constraints, 
        input_value_ids, 
        output_value_ids, 
        scaled_value_ids
    ):
    
    self.used_modules = used_modules
    self.code = code
    self.value_names = value_names 
    self. effects = effects
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

def to_py(self):
    return {"code" : self.code,
            "value_name" : self.value_names,
            "effects" : self.task.effects,
            "pa_union" : [pa.to_py() for pa in self.pa_union],
            "memory_usage" : self.memory_usage,
            "inputs" : self.imput_value_ids,
            "outputs" : self.output_value_ids,
            "keep_same_sample_rate" : self.keep_same_sample_rate
            }