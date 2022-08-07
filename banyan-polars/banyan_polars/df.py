from typing import List

from banyan import (
    compute,
    Distributed,
    Future,
    partitioned_computation,
    pt,
    Replicated,
    sample,
    sample_for_grouping,
)
from plum import conversion_method, convert
import polars.DataFrame

from locations import RemoteTableDestination, RemoteTableSource


class DataFrame:
    def __init__(self, data: Future, nrows: Future):
        self.data = data
        self.nrows = nrows

    @property
    def shape(self):
        return (self.height, self.width)

    @property
    def width(self):
        return sample(DataFrame(self.data, self.nrows)).width

    @property
    def height(self):
        return compute(self.nrows)

    @property
    def schema(self):
        return sample(DataFrame(self.data, self.nrows)).schema

    @property
    def dtypes(self):
        return sample(DataFrame(self.data, self.nrows)).dtypes

    @property
    def columns(self):
        return sample(DataFrame(self.data, self.nrows)).columns


# DataFrame conversion


@conversion_method(type_from=polars.DataFrame, type_to=DataFrame)
def polars_df_to_banyan_df(df):
    return DataFrame(Future(df, datatype="DataFrame"), Future(df.width))


@conversion_method(type_from=DataFrame, type_to=Future)
def df_to_future(df):
    return df.data


# DataFrame sampling


@sample.dispatch
def sample(df: DataFrame):
    return sample(df.data)


# DataFrame creation


def read_table(path: str, **kwargs):
    df_loc = RemoteTableSource(path, kwargs)
    if df_loc.src_name != "Remote":
        raise Exception(f"{path} does not exist")
    df_loc_nrows = df_loc.src_parameters["nrows"]
    df_nrows = Future(df_loc_nrows)
    return DataFrame(Future(datatype="DataFrame", source=df_loc), df_nrows)


def Partitioned(f: Future):
    res = Distributed(sample_for_grouping(f, str))
    res = res.append(Replicated())
    return res


def pts_for_partitioned(futures: List[Future]):
    return pt(futures[0], Partitioned(futures[0]))


def write_table(df: DataFrame, path):
    partitioned_computation(
        pts_for_partitioned,
        df,
        destination=RemoteTableDestination(path),
        new_source=lambda _: RemoteTableSource(path),
    )
