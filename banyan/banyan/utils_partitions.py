from typing import Any, Dict, List, Tuple


from future import Future
from id import ValueId
from id import generate_bang_value


PartitionTypeReference = Tuple[ValueId, int]


class PartitioningConstraint:
    def __init__(self, ty:str, args: List[PartitionTypeReference], co_args: List[List[PartitionTypeReference]], func):
        self.ty = ty
        self.args = args
        self.co_args = co_args
    
    def to_py(self):
        return {"type": self.ty, "args": self.args if self.co_args else self.co_args}

def PartitioningConstraintOverGroup(ty, args):
    return PartitioningConstraint(ty, args, [], lambda x: x)

def PartitioningConstraintOverGroups(ty, co_args):
    return PartitioningConstraint(ty, [], co_args, lambda x: x)

def PartitioningConstraintFunction(func):
    return PartitioningConstraint("FUNCTION", [], [], func)


class PartitioningConstraints:
    def __init__ (self, constraints: List[PartitioningConstraint]):
        self.constraints = constraints

    def to_py (self):
        return {
            "constraints": [
                constraint.to_py()
                for constraint in self.constraints
            ]
        }

PartitionTypeParameters = Dict[str, Any]

class PartitionType:
    def __init__(self, *args):
        if (len(args) == 2) and isinstance(args[0], PartitionTypeParameters) and isinstance(args[1], PartitioningConstraints):
            self.  = args[0]
            self.constraints = args[1]
        else:
            args = args[0]
            parameters = {}
            constraints = PartitioningConstraints()

            # Construct parameters and constraints from arguments
            for arg in args:
                if isinstance(arg, str):
                    parameters["name"] = arg
                elif isinstance(arg, tuple):
                    parameters[arg[0]] = arg[-1]
                elif isinstance(arg, PartitioningConstraint):
                    arg.append(constraints.constraints)
                # TODO - figure out func
                elif isinstance(callable(arg)):
                    PartitioningConstraintFunction(arg).append(constraints.constraints)
                else:
                    raise Exception("Expected either a partition type parameter or constraint")
                self.parameters = parameters

    def to_py(self):
        for (k, v) in self.parameters:
            if v == "!":
                self.parameters[k] = generate_bang_value()
        return {"parameters": self.parameters, "constraints": to_py(self.constraints)}
        



class PartitionTypeComposition:
    def __init__ (self,  pts: List[PartitionType]):
        self.pts = pts

    def to_py (self):
        map(lambda p: p.to_py(), self.pts)

class Partitions:
    def __init__ (self, pt_stacks: Dict[ValueId, PartitionTypeComposition]):
        self.pt_stacks = pt_stacks

    def to_py (self):
        return ("pt_stacks": (v: to_py(self.pt_stacks) for (v, self.ptc) in p.pt_stacks),
    )


class PartitionAnnotation:
    def __init__ (self, partitions: Partitions, constraints, PartitioningConstraints):
        self.partitions = partitions
        self.constraints = constraints

    def to_py (self):
        return {"partitions": to_py(self.partitions), "constraints": to_py(self.constraints)}

class PartitionedUsingFunc:
    def __init__ (self, grouped: List[Future], keep_same_keys: bool, keys: list, keys_by_future: List[Tuple[Future, list]], renamed: bool, drifted: bool, isnothing: bool):
        self.grouped = grouped
        self.keep_same_keys = keep_same_keys
        self.keys = keys
        self.keys_by_future = keys_by_future
        self.renamed = renamed
        self.drifted = drifted
        self.isnothing = isnothing
    
    def is_none(self):
        f.is_none;

class PartionedWithFunc:
    def __init__ (self, func, future_):
        self.func = func
        self.future_ = future_
