class Location:
    def __init__(
        self,
        src_name:str,
        dst_name:str,
        src_parameters:LocationParameters,
        dst_parameters:LocationParameters,
        sample:Sample
    ):
        self.src_name = src_name
        self.dst_name = dst_name
        self.src_parameters = src_parameters
        self.dst_parameters = dst_parameters
        self.sample = sample