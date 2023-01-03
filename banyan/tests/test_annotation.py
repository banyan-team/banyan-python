import banyan as bn
from banyan.utils_future_computation import print_task_graph


def filter_df(x, func=None):
    return x


def test_record_task():
    arg = bn.Future()
    res = bn.Future()
    bn.record_task(
        res,
        filter_df,
        arg,
        [
            {arg: "Blocked", res: "Blocked"},
            {arg: "Grouped", res: "Grouped"},
            {
                arg: bn.PartitionType("Grouped", {"key": "species"}),
                res: "Grouped",
            },
            {arg: "Replicated", res: "Replicated"},
        ],
    )

    arg = res
    res = bn.Future()
    bn.record_task(
        res,
        filter_df,
        arg,
        [{arg: "Replicated", res: "Replicated"}],
        static=res,
    )

    arg = res
    res = bn.Future()
    bn.record_task(
        res,
        filter_df,
        arg,
        [{arg: "Replicated", res: "Replicated"}],
        static=[res],
    )

    assert len(arg._task_graph) == 2
    assert len(res._task_graph) == 3
    assert res._task_graph[0].results[0] == res._task_graph[1].args[0]
    assert len(res._task_graph[0].partitioning_list) == 4
    assert len(arg._task_graph[0].partitioning_list) == 4
    pt = arg._task_graph[0].partitioning_list[2][arg._task_graph[0].args[0]]
    assert pt.is_grouped
    assert pt.params["key"] == "species"

    arg = res
    res = bn.Future()
    bn.record_task(
        res,
        filter_df,
        arg,
        [
            "Blocked",
            "Consolidated",
            {
                arg: bn.PartitionType("Grouped", {"key": "species"}),
                res: "Grouped",
            },
            bn.PartitionType("Grouped", {"key": "species"}),
        ],
    )
    assert len(arg._task_graph) == 3
    assert len(res._task_graph) == 4
    assert len(res._task_graph[3].partitioning_list) == 4
    for pt in [
        res._task_graph[3].partitioning_list[2][res._task_graph[3].args[0]],
        res._task_graph[3].partitioning_list[3][res._task_graph[3].args[0]],
        res._task_graph[3].partitioning_list[3][res._task_graph[3].results[0]],
    ]:
        assert pt.is_grouped
        assert pt.params["key"] == "species"


def test_record_task_with_constants():
    arg = bn.Future()
    res = bn.Future()
    bn.record_task(
        [res],
        filter_df,
        [arg, lambda x: x * 2],
        [
            {arg: "Blocked", res: "Blocked"},
            {arg: "Grouped", res: "Grouped"},
            {
                arg: bn.PartitionType("Grouped", {"key": "species"}),
                res: "Grouped",
            },
            {arg: "Replicated", res: "Replicated"},
        ],
    )
    assert len(res._task_graph[0].args) == 2
