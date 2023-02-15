import banyan as bn
import banyan_polars as bpl
import polars as pl


def test_task_graph_construction():
    df = bpl.read_csv("s3://test-bucket")
    df = df.select(pl.col("species") == "iris")
    tg = bn.annotation.to_future(df)._task_graph
    assert len(tg) == 2
    assert tg[0].name.startswith("LocationSpec")
    assert tg[1].name.startswith("select")
