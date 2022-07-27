from email.errors import MultipartInvariantViolationDefect
from multiprocessing.sharedctypes import Value
from os import startfile
from typing import Any

from partitions import ValueId

class Future: #TODO what is AbstractFuture?
    """??
    """
    def __init__ (
        self,
        datatype: str, 
        value: Any, 
        value_id: ValueId, 
        mutated: bool,
        stale: bool,
        total_memory_usage: int
    ):

        self.datatype = datatype
        self.value = value
        self.value_id = value_id
        self.mutated = mutated
        self.stale = stale
        self.total_memory_usage = total_memory_usage  
    
    def __hash__(self) -> int:
        return hash(self.value_id)

    def is_none(self) -> bool:
        return len(self.value_id) == 0

    def __del__(self):
        _finalize_future(self)

NOTHING_FUTURE = Future("", None, "", False, False, -1)

def _finalize_future(fut: Future):
    session_id = _get_session_id_no_error()
    sessions_dict = get_sessions_dict()
    if len(session_id) == 0 and sessions_dict.haskey(session_id)
        destroy_future(fut)

def create_future(datatype:str, value:Any, value_id:ValueId, mutated:bool, stale::bool):
    new_future = Future(datatype, value, value_id, mutated, stale, -1)
    return new_future

def value_id_getter(f):
    return value_id