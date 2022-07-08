from samples import sample_memory_usage
from session import get_session

class Sample:
    def __init__(self, value = None, memory_usage = None, rate = None):
        self.groupingkeys = []
        self.value = value
        self.objectid = hash(value)

        if (value is None) and (memory_usage is None) and (rate is None):
            self.memory_usage = 0
            self.rate = get_session().sample_rate
        elif (value is not None) and (memory_usage is None) and (rate is None):
            self.memory_usage = sample_memory_usage(value)
            self.rate = get_session().sample_rate
        elif (value is not None) and (memory_usage is not None) and (rate is None):
            sample_rate = get_session().sample_rate
            self.memory_usage = int(round(memory_usage / sample_rate))
            self.rate = sample_rate
        else:
            self.memory_usage = memory_usage
            self.rate = rate

       
        
