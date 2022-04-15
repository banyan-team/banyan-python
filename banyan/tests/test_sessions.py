import boto3
from contextlib import nullcontext as does_not_raise

import os
import pytest
import time

from banyan.clusters import get_cluster_s3_bucket_name
from banyan.sessions import (
    get_session,
    get_sessions,
    get_session_status,
    start_session,
    get_running_sessions,
    end_session,
    get_running_sessions,
    run_session,
)

TEST_BRANCH = "claris+melany/banyan-python"


@pytest.mark.parametrize(
    "status", ["all", "creating", "running", "failed", "completed", "invalid_status"]
)
def test_get_sessions_with_status(status):
    """Test getting sessions for a cluster."""
    cluster_name = os.environ["BANYAN_CLUSTER_NAME"]
    if status == "all":
        sessions = get_sessions(cluster_name)
    else:
        filtered_sessions = get_sessions(cluster_name, status=status)
        assert all([s["status"] == status for (s_id, s) in filtered_sessions.items()])


def test_start_get_end_sessions():
    # Start a session
    cluster_name = os.environ["BANYAN_CLUSTER_NAME"]

    session_id = start_session(
        cluster_name=cluster_name,
        nworkers=2,
        url="https://github.com/banyan-team/banyan-python",
        branch=TEST_BRANCH,
        directory="banyan-python/banyan",
        force_sync=(os.getenv("BANYAN_FORCE_SYNC") == "1"),
    )
    running_sessions = get_running_sessions(cluster_name)
    all_sessions = get_sessions(cluster_name)
    end_session(session_id, release_resources_now=True)
    running_sessions_after = get_running_sessions(cluster_name)
    all_sessions_after = get_sessions(cluster_name)

    # Before end_session
    assert all([s["status"] == "running" for (s_id, s) in running_sessions.items()])
    assert session_id in running_sessions
    assert session_id in all_sessions
    # After end_session
    assert (session_id in all_sessions_after) and (all_sessions_after[session_id]["status"] == "completed")
    assert session_id not in running_sessions_after


# @testset "Start a session with dev paths" begin
#     session_id = start_session(
#         cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#         nworkers = 2,
#         url = "https://github.com/banyan-team/banyan-julia.git",
#         branch = get(ENV, "BANYAN_JULIA_BRANCH", Banyan.get_branch_name()),
#         directory = "banyan-julia/Banyan/test",
#         dev_paths = [
#             "banyan-julia/Banyan",
#         ],
#         force_pull = true,
#         force_sync = true,
#         force_install = true,
#         store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
#     )
#     session_status = get_session_status(session_id)
#     end_session(session_id, release_resources_now=true)
#     @test session_status == "running"
# end


@pytest.mark.parametrize("nowait", [True, False])
def test_start_sessions_with_nowait(nowait):
    cluster_name = os.environ["BANYAN_CLUSTER_NAME"]

    session_id = start_session(
        cluster_name=cluster_name,
        nworkers=2,
        store_logs_on_cluster=os.environ.get("BANYAN_STORE_LOGS_ON_CLUSTER", "0")
        == "1",
        url="https://github.com/banyan-team/banyan-python",
        branch=TEST_BRANCH,
        directory="banyan-python/banyan",
        force_sync=os.getenv("BANYAN_FORCE_SYNC") == "1",
        nowait=nowait,
    )

    session_status = get_session_status(session_id)
    if not nowait:
        assert session_status == "running"
    else:
        assert session_status == "creating"
        while session_status == "creating":
            time.sleep(20)
            session_status = get_session_status(session_id)
        assert session_status == "running"

    end_session(session_id, release_resources_now=True)


@pytest.mark.parametrize("estimate_available_memory", [True, False])
def test_start_sessions_with_estimate_available_memory(estimate_available_memory):
    cluster_name = os.environ["BANYAN_CLUSTER_NAME"]

    session_id = start_session(
        cluster_name=cluster_name,
        nworkers=2,
        store_logs_on_cluster=os.environ.get("BANYAN_STORE_LOGS_ON_CLUSTER", "0")
        == "1",
        url="https://github.com/banyan-team/banyan-python",
        branch=TEST_BRANCH,
        directory="banyan-python/banyan",
        force_sync=os.getenv("BANYAN_FORCE_SYNC") == "1",
        estimate_available_memory=estimate_available_memory,
    )

    end_session(session_id, release_resources_now=True)


@pytest.mark.parametrize("store_logs_in_s3", [True, False])
def test_start_sessions_store_logs_in_s3(store_logs_in_s3):
    cluster_name = os.environ["BANYAN_CLUSTER_NAME"]

    session_id = start_session(
        cluster_name=cluster_name,
        nworkers=2,
        url="https://github.com/banyan-team/banyan-python",
        branch=TEST_BRANCH,
        directory="banyan-python/banyan",
        store_logs_in_s3=store_logs_in_s3,
        force_sync=os.getenv("BANYAN_FORCE_SYNC") == "1",
    )
    end_session(session_id, release_resources_now=True)
    time.sleep(60)

    log_file = f"banyan-log-for-session-{session_id}"
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(get_cluster_s3_bucket_name(cluster_name))
    objs = list(bucket.objects.filter(Prefix=log_file))
    assert store_logs_in_s3 == (len(objs) > 0)


def test_start_end_multiple_sessions():
    cluster_name = os.environ["BANYAN_CLUSTER_NAME"]
    delay_time = 5

    # Start a session and end it
    session_id_1 = start_session(
        cluster_name=cluster_name,
        nworkers=2,
        url="https://github.com/banyan-team/banyan-python",
        branch=TEST_BRANCH,
        directory="banyan-python/banyan",
        force_sync=True,
        store_logs_on_cluster=os.environ.get("BANYAN_STORE_LOGS_ON_CLUSTER", "0")
        == "1",
        release_resources_after=delay_time,
    )
    resource_id_1 = get_session().resource_id
    session_status = get_session_status(session_id_1)
    assert session_status == "running"

    end_session(session_id_1)
    time.sleep(60)  # To ensure session gets ended
    session_status = get_session_status(session_id_1)
    assert session_status == "completed"

    # Start another session with same nworkers and verify the job ID matches
    session_id_2 = start_session(
        cluster_name=os.environ["BANYAN_CLUSTER_NAME"],
        nworkers=2,
        url="https://github.com/banyan-team/banyan-python",
        branch=TEST_BRANCH,
        directory="banyan-python/banyan",
        store_logs_on_cluster=os.environ.get("BANYAN_STORE_LOGS_ON_CLUSTER", "0")
        == "1",
        release_resources_after=delay_time,
    )
    resource_id_2 = get_session().resource_id
    session_status = get_session_status(session_id_2)
    assert session_status == "running"
    assert resource_id_2 == resource_id_1  # it should have reused resource

    end_session(session_id_2)
    time.sleep(60)
    session_status = get_session_status(session_id_2)
    assert session_status == "completed"

    # Start another session with different nworkers and verify the job ID
    # is different
    session_id_3 = start_session(
        cluster_name=os.environ["BANYAN_CLUSTER_NAME"],
        nworkers=4,
        url="https://github.com/banyan-team/banyan-python",
        branch=TEST_BRANCH,
        directory="banyan-python/banyan",
        store_logs_on_cluster=os.environ.get("BANYAN_STORE_LOGS_ON_CLUSTER", "0")
        == "1",
        release_resources_after=delay_time,
    )
    resource_id_3 = get_session().resource_id
    session_status = get_session_status(session_id_3)
    assert session_status == "running"
    assert resource_id_3 != resource_id_1

    end_session(session_id_3)
    time.sleep(60)
    session_status = get_session_status(session_id_3)
    assert session_status == "completed"

    # Sleep for the delay_time and check that the underlying resources are destroyed
    # by creating a new session and ensuring that it uses different resources
    time.sleep(delay_time * 60)
    session_id_4 = start_session(
        cluster_name=os.environ["BANYAN_CLUSTER_NAME"],
        nworkers=2,
        url="https://github.com/banyan-team/banyan-python",
        branch=TEST_BRANCH,
        directory="banyan-python/banyan",
        store_logs_on_cluster=os.environ.get("BANYAN_STORE_LOGS_ON_CLUSTER", "0")
        == "1",
        release_resources_after=delay_time,
        nowait=True,
    )
    resource_id_4 = get_session().resource_id
    assert resource_id_4 != resource_id_1

    end_session(session_id_4, release_resources_now=True)


def test_start_session_with_invalid_branch_name():
    cluster_name = os.environ["BANYAN_CLUSTER_NAME"]
    with pytest.raises(Exception):
        session_id = start_session(
            cluster_name=cluster_name,
            nworkers=2,
            store_logs_on_cluster=os.environ.get("BANYAN_STORE_LOGS_ON_CLUSTER", "0")
            == "1",
            url="https://github.com/banyan-team/banyan-python",
            branch="nonexistant-branch",
            directory="banyan-python/banyan",
            force_sync=os.getenv("BANYAN_FORCE_SYNC") == "1",
        )
    try:
        end_session(session_id, release_resources_now=True)
    except:
        pass


def test_run_session_with_mpi_script():
    cluster_name = os.getenv("BANYAN_CLUSTER_NAME")
    with does_not_raise():
        run_session(
            cluster_name=cluster_name,
            nworkers=2,
            url="https://github.com/banyan-team/banyan-python",
            branch=TEST_BRANCH,
            directory="banyan-python/banyan",
            code_files=["tests/mpi_script_success.py"],
            force_sync=os.getenv("BANYAN_FORCE_SYNC") == "1",
            store_logs_on_cluster=os.environ.get("BANYAN_STORE_LOGS_ON_CLUSTER", "0")
            == "1",
        )

def test_run_session_with_mpi_script_with_error():
    cluster_name = os.getenv("BANYAN_CLUSTER_NAME")
    with pytest.raises(Exception):
        run_session(
            cluster_name=cluster_name,
            nworkers=2,
            url="https://github.com/banyan-team/banyan-python",
            branch=TEST_BRANCH,
            directory="banyan-python/banyan",
            code_files=["tests/mpi_script_fail.py"],
            force_sync=os.getenv("BANYAN_FORCE_SYNC") == "1",
            store_logs_on_cluster=os.environ.get("BANYAN_STORE_LOGS_ON_CLUSTER", "0")
            == "1",
        )

   

# @testset "Reusing session that fails" begin
#     Pkg.activate("./")
#     cluster_name = ENV["BANYAN_CLUSTER_NAME"]

#     # Start a session
#     session_id_1 = start_session(
#         cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#         nworkers = 2,
#         store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
#         force_sync=true
#     )
#     resource_id_1 = get_session().resource_id
#     session_status_1 = get_session_status(session_id_1)

#     # Trigger a failure in the session that will end the session
#     try
#         @test_throws begin
#             offloaded(distributed=true) do
#                 error("Oops sorry this is an error")
#             end
#         end ErrorException
#     catch
#     end
#     session_status_1_after_failure = get_session_status(session_id_1)

#     # Start a new session (it should reuse the resources of the failed session) and then end it
#     session_id_2 = start_session(
#         cluster_name = ENV["BANYAN_CLUSTER_NAME"],
#         nworkers = 2,
#         store_logs_on_cluster=get(ENV, "BANYAN_STORE_LOGS_ON_CLUSTER", "0") == "1",
#         nowait=true
#     )
#     resource_id_2 = get_session().resource_id
#     session_status_2 = get_session_status(session_id_2)
#     end_session(session_id_2, release_resources_now=true)

#     # Assert
#     @test session_status_1 == "running"
#     @test session_status_1_after_failure == "failed"
#     @test resource_id_2 == resource_id_1
# end






