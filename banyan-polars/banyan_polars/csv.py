from plum import dispatch
import polars as pl

from banyan import (
    file_ending,
    get_sample,
    get_sample_and_metadata,
    get_sample_from_data,
    has_separate_metadata,
    sample_from_range,
    write_file,
)


class CSV:
    pass


@dispatch
def has_separate_metadata(t: CSV):
    return False


@dispatch
def get_sample(t: CSV, p, sample_rate, l):
    rand_indices = sample_from_range(range(l), sample_rate)
    if (sample_rate != 1.0) and (len(rand_indices) == 0):
        return pl.DataFrame()
    else:
        return get_sample_from_data(
            pl.read_csv(p, header=1, skip_rows=1), sample_rate, rand_indices
        )


@dispatch
def get_sample_and_metadata(t: CSV, p, sample_rate):
    sample_df = pl.read_csv(p, header=1, skip_rows=1)
    num_rows = sample_df.nrows
    return get_sample_from_data(sample_df, sample_rate, num_rows), num_rows


@dispatch
def file_ending(t: CSV):
    return "csv"


@dispatch
def read_file(t: CSV, path, rowrange, readrange, filerowrange):
    return pl.read_csv(
        path,
        header=1,
        skip_rows=(readrange.start - filerowrange.start + 1),
        n_rows=(readrange.stop - readrange.start + 1),
    )


@dispatch
def read_file(t: CSV, path):
    return pl.read_csv(path)


@dispatch
def write_file(t: CSV, part, path, nrows):
    part.write_csv(path)
