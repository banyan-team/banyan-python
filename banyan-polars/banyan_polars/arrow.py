from regex import R
from plum import dispatch
import polars as pl
import pyarrow as pa
import pyarrow.dataset as pd


from banyan import (
    file_ending,
    get_sample,
    get_sample_and_metadata,
    get_sample_from_data,
    has_separate_metadata,
    read_file,
    sample_from_range,
    write_file,
)


class Arrow:
    pass


def PyArrowTable(p):
    with pa.memory_map(p, "rb") as source:
        return pa.ipc.open_file(source).read_all()


@dispatch
def has_separate_metadata(t: Arrow):
    return True


@dispatch
def get_metadata(t: Arrow, p) -> int:
    return pd.dataset(p).count_rows()


@dispatch
def get_sample(t: Arrow, p, sample_rate, l):
    rand_indices = sample_from_range(range(l), sample_rate)
    if (sample_rate != 1.0) and (len(rand_indices) == 0):
        return pl.DataFrame()
    else:
        return get_sample_from_data(
            pl.from_arrow(pd.dataset(p).to_table()), sample_rate, rand_indices
        )


def get_sample_and_metadata(t: Arrow, p, sample_rate):
    sample_df = pl.from_arrow(pd.dataset(p).to_table())
    num_rows = sample_df.nrows
    return get_sample_from_data(sample_df, sample_rate, num_rows), num_rows


@dispatch
def file_ending(t: Arrow):
    return "arrow"


@dispatch
def read_file(t: Arrow, path: str, rowrange, readrange, filerowrange):
    unfiltered = pl.from_arrow(pd.dataset(path).to_table())
    starti = readrange.start - filerowrange.start + 1
    endi = readrange.stop - filerowrange.start + 1
    read_whole_file = (starti == 0) and (endi == filerowrange.stop)
    return (
        unfiltered if read_whole_file else unfiltered[starti:endi, :]
    )  # TODO: Use filter instead?


@dispatch
def read_file(t: Arrow, path: str):
    unfiltered = pl.from_arrow(pd.dataset(path).to_table())
    return unfiltered


@dispatch
def write_file(t: Arrow, part: pl.DataFrame, path, nrows):
    part.write_ipc(path, compression="zstd")
