from operator import truediv
import os

from banyan.sessions import (
    get_sessions,
    start_session,
    get_running_sessions,
    end_session,
    get_running_sessions,
    run_session,
)


def test_get_sessions():
    """Will test getting sessions, starting, running, and ending it"""
    cluster_name = os.environ["BANYAN_CLUSTER_NAME"]
    get_sessions(cluster_name)


def test_start_get_end():
    cluster_name = os.environ["BANYAN_CLUSTER_NAME"]
    session_id = start_session(
        cluster_name=cluster_name,
        nworkers=16,
        url="https://github.com/banyan-team/banyan-python",
        branch="v22.02.13",
        directory="banyan",
        nowait=True,
    )

    assert get_sessions().has_key(session_id)

    assert get_running_sessions().has_key(session_id)

    end_session(session_id)

    assert get_sessions().has_key(session_id)

    assert not get_running_sessions().has_key(session_id)


def test_run_session():
    name = os.getenv("BANYAN_CLUSTER_NAME")
    print("nameee: " + str(name))
    print("FORCE_SYNC: ", os.getenv("BANYAN_FORCE_SYNC") == "1")
    run_session(
        nworkers=2,
        cluster_name=name,
        url="https://github.com/banyan-team/banyan-python",
        branch="claris+melany/banyan-python",
        directory="banyan-python/banyan",
        # dev_paths=["banyan-python/banyan/"],
        force_sync=os.getenv("BANYAN_FORCE_SYNC") == "1",
    )
