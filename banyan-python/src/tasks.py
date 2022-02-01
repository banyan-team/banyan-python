

class DelayedTask:
    def __init__(
        self,
        used_modules:List,
        code:str,
        value_names:List[Tuple[ValueId,str]],
        effects:Dict[ValueId,str],
        pa_union,#:List[PartitionAnnotation],
        partitioned_using_func:Union[function,None],
        partitioned_with_func:Union[function,None],
        mutation:Dict# Dict[future, future]?
    ):
        self.used_modules = used_modules
        self.code = code
        self.value_names = value_names
        self.effects = effects
        self.pa_union = pa_union
        self.partitioned_using_func = partitioned_using_func
        self.partitioned_with_func = partitioned_with_func
        self.mutation = mutation