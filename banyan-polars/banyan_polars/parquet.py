from plum import dispatch
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from banyan import (
    file_ending,
    get_sample,
    get_sample_and_metadata,
    get_sample_from_data,
    has_separate_metadata,
    sample_from_range,
    write_file,
)


class Parquet:
    pass


@dispatch
def has_separate_metadata(t: Parquet):
    return True


@dispatch
def get_metadata(t: Parquet, p) -> int:
    return pq.read_metadata(p).num_rows


@dispatch
def get_sample(t: Parquet, p, sample_rate, l):
    rand_indices = sample_from_range(range(l), sample_rate)
    if (sample_rate != 1.0) and (len(rand_indices) == 0):
        return pl.DataFrame()
    else:
        try:
            return get_sample_from_data(
                pl.read_parquet(p, n_rows=l), sample_rate, rand_indices
            )
        except:
            return pl.DataFrame()


@dispatch
def file_ending(t: Parquet):
    return "parquet"


def read_file(t: Parquet, path: str, rowrange, readrange, filerowrange):
    try:
        # We read from the start of the file to the last index. Then, we drop
        # the data before the start index.
        return pl.read_parquet(path)
        # rows = (readrange.start-filerowrange.start+1):(readrange.stop-filerowrange.start+1),
    except:
        # File does not exist
        if path.startswith("efs/s3/"):
            raise Exception(f'Path "{path}" should not start with "s3/"')
        return pl.DataFrame()


@dispatch
def read_file(t: Parquet, path: str):
    try:
        return pl.read_parquet(path)
    except:
        if path.startswith("efs/s3/"):
            raise Exception(f'Path "{path}" should not start with "s3/"')
        return pl.DataFrame()


@dispatch
def write_file(t: Parquet, part: pl.DataFrame, path, nrows):
    if nrows > 0:
        part.write_parquet(path)
