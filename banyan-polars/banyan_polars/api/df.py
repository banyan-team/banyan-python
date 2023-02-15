import banyan as bn
import polars as pl
from typing_extensions import Self

from ..communication.lazy_aggregation import LazyAggregation
from .utils_constants import AGGREGATION_FUNCTIONS

# class GroupBy:
#     def __init__(self, fut: bn.Future):
#         self.future = fut

#     def __future__(self) -> bn.Future:
#         return self.future

#     def agg(self, cols):
#         return bn.record_task(
#             "res",
#             LazyAggregation,
#             [self, pl.internals.dataframe.groupby.GroupBy.agg, ],
#             ["Blocked", "Consolidated", "Grouped"],
#         )


def is_aggregation(expr) -> bool:
    if isinstance(expr, list):
        return all(is_aggregation(e) for e in expr)
    if isinstance(expr, str):
        return False
    expr_str = str(expr)
    return any(s in expr_str for s in AGGREGATION_FUNCTIONS)


class DataFrame:
    def __init__(self, fut: bn.Future):
        self.future = fut

    def __future__(self) -> bn.Future:
        return self.future

    # def filter(self, expr) -> Self:
    #     return DataFrame(
    #         bn.record_task(
    #             "res",
    #             pl.DataFrame.filter,
    #             [self, expr],
    #             ["Blocked", "Consolidated", "Grouped"],
    #         )
    #     )

    def select(self, expr) -> Self:
        if is_aggregation(expr):
            raise ValueError(
                f"select received expression {str(expr)} that has an aggregation function not currently supported"
            )
        return DataFrame(
            bn.record_task(
                "res",
                pl.DataFrame.select,
                [self, expr],
                ["Blocked", "Consolidated", "Grouped"],
            )
        )

    # def groupby(self, cols) -> GroupBy:
    #     keys = [col for col in bn.utils.to_list(cols)]
    #     # TODO: Convert keys to strings if they are columns/expressions
    #     keys_grouping_pts = [bn.pt("Grouped", key=key for key in keys]
    #     return GroupBy(
    #         bn.record_task(
    #             "res",
    #             pl.DataFrame.groupby,
    #             [self, cols],
    #             ["Blocked", "Consolidated", *keys_grouping_pts],
    #         )
    #     )
