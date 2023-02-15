class LazyAggregation:
    """
    Store information to lazily aggregate data across multiple workers when
    the future for the `LazyAggregation` is converted from None to Consolidated
    partition type.
    """

    def __init__(
        self,
        data,
        data_func,
        value_func,
        data_func_args=None,
        value_func_args=None,
    ):
        self.data = data
        self.data_func = data_func
        self.value_func = value_func
        self.data_func_args = data_func_args if data_func_args is None else []
        self.value_func_args = (
            value_func_args if value_func_args is None else []
        )

    # TODO: Add `convert_partition_type` implementation here
