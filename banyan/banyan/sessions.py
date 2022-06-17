import boto3
import logging
import os
from pygit2 import Repository
import time
from tqdm import tqdm


from .constants import BANYAN_PYTHON_BRANCH_NAME, BANYAN_PYTHON_PACKAGES
from .clusters import get_running_clusters, get_cluster_s3_bucket_name, wait_for_cluster
from .config import configure
from .session import (
    get_session_id,
    set_session,
    sessions,
    current_session_id,
    get_session,
    Session,
)
from .utils import (
    get_hash,
    get_loaded_packages,
    get_python_version,
    load_toml,
    parse_time,
    send_request_get_response,
    upload_file_to_s3,
)


def start_session(
    cluster_name: str = None,
    nworkers: int = 16,
    release_resources_after: int = 20,
    print_logs: bool = False,
    store_logs_in_s3: bool = True,
    store_logs_on_cluster: bool = False,
    log_initialization: bool = False,
    sample_rate: int = None,
    session_name: str = None,
    files: list = None,
    code_files: list = None,
    force_update_files: bool = False,
    pf_dispatch_table: list[str] = None,
    using_modules: list = None,
    # pip_requirements_file = None, # paths to a requirements.txt file that contains packages to be installed with pip
    # conda_environment_file = None, # paths to environment.yml file that contains packages to be installed with conda
    project_dir: str = None,  # a pyproject.toml file containing information about a poetry environment
    url: str = None,
    branch: str = None,
    directory: str = None,
    dev_paths: list = None,
    force_sync: bool = False,
    force_pull: bool = False,
    force_install=False,
    estimate_available_memory=True,
    nowait=True,
    email_when_ready=None,
    for_running=False,
    *args,
    **kwargs,
):
    """
    Starts a new session.
    """

    configure(*args, **kwargs)

    if sample_rate is None:
        sample_rate = nworkers

    if files is None:
        files = []

    if code_files is None:
        code_files = []

    if using_modules is None:
        using_modules = []

    if dev_paths is None:
        dev_paths = []

    if project_dir is None:
        project_dir = os.getcwd()  # gets the current working directory

    poetry_pyproject_file = os.path.join(project_dir, "pyproject.toml")
    poetry_lock_file = os.path.join(project_dir, "poetry.lock")

    # Construct parameters for starting session
    if cluster_name is None:
        # running_clusters is dictionary
        running_clusters = get_running_clusters()
        if len(running_clusters) == 0:
            raise Exception(
                "Failed to start session: you don't have any clusters created"
            )
        else:
            cluster_name = list(running_clusters.keys())[0]
    version = get_python_version()

    session_configuration = {
        "cluster_name": cluster_name,
        "num_workers": nworkers,
        "release_resources_after": release_resources_after,
        "return_logs": print_logs,
        "store_logs_in_s3": store_logs_in_s3,
        "store_logs_on_cluster": store_logs_on_cluster,
        "version": version,
        "benchmark": os.environ.get("BANYAN_BENCHMARK", "0") == "1",
        "main_modules": get_loaded_packages(),
        "using_modules": using_modules,
        "reuse_resources": not force_update_files,
        "estimate_available_memory": estimate_available_memory,
        "language": "py",
    }

    if session_name is None:
        session_configuration["session_name"] = session_name

    if email_when_ready is None:
        session_configuration["email_when_ready"] = email_when_ready

    s3_bucket_name = get_cluster_s3_bucket_name(cluster_name)
    environment_info = {}
    # If a url is not provided, then use the local environment
    if url is None:
        # There are two files we need: pyproject.toml and project.lock
        # Check if the pyproject.toml exists
        if os.path.exists(poetry_pyproject_file):
            # Check if the project.lock exists
            if os.path.exists(poetry_lock_file):
                # Read in the poetry.lock
                with open(poetry_lock_file) as f:
                    poetry_lock_file_contents = f.read()
            else:
                # If it doesn't exist - that's fine..
                poetry_lock_file_contents = ""
            # Read in the pyproject.toml
            with open(poetry_pyproject_file) as f:
                file_contents = f.read()

            # At this point, both files have been read in so we go ahead and
            # get the hash of them concatenated
            environment_hash = get_hash(poetry_lock_file_contents + file_contents)
            environment_info["environment_hash"] = environment_hash

            # Upload the pyproject.toml file to S3
            object_name = environment_hash + "/pyproject.toml"
            upload_file_to_s3(poetry_pyproject_file, s3_bucket_name, object_name)
            environment_info["pyproject_toml"] = object_name

            if poetry_lock_file_contents != "":
                object_name = environment_hash + "/poetry.lock"
                upload_file_to_s3(poetry_lock_file, s3_bucket_name, object_name)
                environment_info["poetry_lock"] = object_name

        else:
            # It has to exist!
            raise Exception("poetry_pyproject_file does not exist")

    else:
        # Otherwise, use url and optionally a particular branch
        environment_info["url"] = url

        if directory is None:
            raise Exception("Directory must be provided for given URL $url")

        environment_info["directory"] = directory

        if branch is not None:
            environment_info["branch"] = branch

        environment_info["dev_paths"] = dev_paths
        environment_info["force_sync"] = force_sync
        environment_info["force_pull"] = force_pull
        environment_info["force_install"] = False  # force_install
        environment_info["environment_hash"] = get_hash(
            url + ("" if branch is None else branch)
        )

    session_configuration["environment_info"] = environment_info

    # Upload files to S3
    for f in files:
        upload_file_to_s3(f.replace("file://", ""), s3_bucket_name)
    for f in code_files:
        upload_file_to_s3(f.replace("file://", ""), s3_bucket_name)

    session_configuration["files"] = [os.path.basename(f) for f in files]
    session_configuration["code_files"] = [os.path.basename(f) for f in code_files]

    if pf_dispatch_table is None:
        # is_it_a ? a : b in Julia becomes a if is_it_a else b in Python (edited)
        branch_to_use = (
            Repository(".").head.shorthand
            if os.getenv("BANYAN_TESTING", "0") == "1"
            else BANYAN_PYTHON_BRANCH_NAME
        )
        pf_dispatch_table = [
            "https://raw.githubusercontent.com/banyan-team/banyan-python/"
            + branch_to_use
            + "/"
            + dir
            + "/res/pf_dispatch_table.toml"
            for dir in BANYAN_PYTHON_PACKAGES
        ]

    pf_dispatch_table_loaded = load_toml(pf_dispatch_table)
    session_configuration["pf_dispatch_table"] = pf_dispatch_table_loaded
    session_configuration["language"] = "py"

    # Start the session
    response = send_request_get_response("start-session", session_configuration)
    session_id = response["session_id"]
    resource_id = response["resource_id"]

    # Store in global state
    set_session(
        session_id,
        Session(cluster_name, session_id, resource_id, nworkers, sample_rate),
    )

    wait_for_cluster(cluster_name)

    if not nowait:
        wait_for_session(session_id)

    return session_id


def end_session(
    session_id=None,
    failed=False,
    release_resources_now=False,
    release_resources_after=None,
    *args,
    **kwargs,
):
    """Ends a session given the session_id.

    Parameters
    ----------
    session_id : string
        Session ID of the session that should get ended.
        Defaults to the session_id returned from get_session_id()
    failed : bool
        Indicates whether the session being ended has failed. Defaults to False.
    release_resources_now : string
        Indicates whether to release underlying resources now. Defaults to False.
    release_resources_after: int
        The number of minutes after which to release underlying resources

    Returns
    -------
    String
        session ID of the session that was ended
    """

    configure(*args, **kwargs)

    if session_id is None:
        session_id = get_session_id()

    # Ending session with ID session_ID
    request_params = {
        "session_id": session_id,
        "failed": failed,
        "release_resources_now": release_resources_now,
    }
    if release_resources_after is not None:
        request_params["release_resources_after"] = release_resources_after
    send_request_get_response("end-session", request_params)

    # Remove from global state
    set_session(None)
    if session_id in sessions:
        del sessions[session_id]
    return session_id


def end_all_sessions(
    cluster_name,
    release_resources_now=False,
    release_resources_after=None,
    *args,
    **kwargs,
):
    """End all running sessions for a given cluster.

    Parameters
    ----------
    session_id : string
        Session ID of the session that should get ended.
        Defaults to the session_id returned from get_session_id()
    failed : bool
        Indicates whether the session being ended has failed. Defaults to False.
    release_resources_now : string
        Indicates whether to release underlying resources now. Defaults to False.
    release_resources_after: int
        The number of minutes after which to release underlying resources
    """

    configure(*args, **kwargs)

    sessions = get_sessions(cluster_name, status=["creating", "running"])
    for (session_id, session) in sessions.items():
        end_session(session_id, release_resources_now, release_resources_after)


def get_sessions(cluster_name=None, status=None, limit=-1, *args, **kwargs):
    """Gets information about all the sessions for the user. Optionally can filter
    by cluster name and status.

    Parameters
    ----------
    cluster_name : string
        Name of the cluste to filter by. Defaults to nothing
    status : string
        Status of session to filter by. Defaults to nothing

    Returns
    -------
    Dictionary
        Mappings from session ID to another dictionary containing information about the session
    """

    configure(*args, **kwargs)

    filters = {}
    if cluster_name is not None:
        filters["cluster_name"] = cluster_name

    if status is not None:
        filters["status"] = status
    # The function repeatedly calls the send_request_get_response function that takes
    # the string 'describe_sessions' and the dictionary that contains filters. looping until
    #'last_eval' does not exist in the indiv_response dictionary.
    if limit > 0:
        # Get the last `limit` number of sessions
        indiv_response = send_request_get_response(
            "describe-sessions", {"filters": filters, "limit": limit}
        )
        sessions = indiv_response["sessions"]
    else:
        # Get all sessions
        indiv_response = send_request_get_response(
            "describe-sessions", {"filters": filters}
        )
        curr_last_eval = indiv_response["last_eval"]
        sessions = indiv_response["sessions"]

        while curr_last_eval is not None:
            indiv_response = send_request_get_response(
                "describe-sessions",
                {"filters": filters, "this_start_key": curr_last_eval},
            )
            sessions.update(indiv_response["sessions"])
            curr_last_eval = indiv_response["last_eval"]

    # sessions is a dictionary that contains
    # mappings from session_id to another dictionary containing information about the session
    #                             {"start_time": "0126202220937124", "end_time": "", "num_workers": 2}
    # both start_time and end_time are of the format "yyyy-mm-dd-HH:MM:SSzzzz"
    for id, s in sessions.items():  # iterating over key and value
        if sessions[id]["end_time"] == "":
            sessions[id]["end_time"] = None
        else:
            sessions[id]["end_time"] = parse_time(sessions[id]["end_time"])
        sessions[id]["start_time"] = parse_time(sessions[id]["start_time"])
    return sessions


def get_running_sessions(*args, **kwargs):
    """Gets info about all sessions that are currently running

    Returns
    -------
    Dictionary
        Mappings from session ID to another dictionary containing information about sessions that are running
    """
    configure(*args, **kwargs)
    return get_sessions(status="running")


def get_session_status(session_id=None, *args, **kwargs):
    """Get the status of the session with the given session ID or current session
    if nothing is provided.

    Parameter
    ---------
    session_id : string
        Session ID of the session that should be got.
        Defaults to the session_id returned from get_session_id()

    Returns
    -------
    string
        Status of the session. If the status is 'failed', the 'status_explanation' is printed
    """

    configure(*args, **kwargs)

    if session_id is None:
        session_id = get_session_id()
    filters = {"session_id": session_id}  # filters is a dictionary
    # response contains a dictionary (at the key "sessions") which maps from session IDs
    # to session information where the session information is a dictionary containing
    # various info such as the status (at the key "status").
    response = send_request_get_response("describe-sessions", {"filters": filters})
    session_status = response["sessions"][session_id]["status"]
    if session_status == "failed":
        # We don't immediately fail - we're just explaining. It's only later on
        # where it's like we're actually using this session do we set the status.
        # TODO: Should this be logging.error?
        # This print statement is necessary so that we can print reason for session failure
        print(response["sessions"][session_id]["status_explanation"])
    return session_status


def download_session_logs(session_id, cluster_name, filename=None, *args, **kwargs):
    """Downloads the logs from Amazon S3 for a particular session to a local file

    Parameters
    ----------
    session_id : string
        Session ID of the session to get the log for that should get downloaded
    clusterr_name : string
        Name of the cluster the session was running on
    fileneame : string and defaults to nothing
        Path to the file on the local computer to which to download to
    """

    configure(*args, **kwargs)

    s3_bucket_name = get_cluster_s3_bucket_name(cluster_name)
    log_file_name = f"banyan-log-for-session-{session_id}"  # This creates a string with the {session_id} replaced with the value of the job_id
    if filename is None:  # if fileneame is not specified
        filename = os.path.expanduser("~")  # path to the home directory on the computer
    s3 = boto3.client("s3")
    s3.download_file(s3_bucket_name, log_file_name, filename)


def wait_for_session(session_id=None, *args, **kwargs):
    """Implements an algorithm to repeatedly get the session status and then wait for a
    period of time

    Parameters
    ----------
    session_id : string
        Session ID of the session that should get ended.
        Defaults to the session_id returned from get_session_id()

    Raises
    ------
    Raises Exception if session fails
    """

    configure(*args, **kwargs)

    if session_id is None:
        session_id = get_session_id()
    session_status = get_session_status(session_id)
    t = 5
    i = 0
    if session_status == "creating":
        pbar = tqdm(desc=f"Starting session with ID {session_id}")
    while session_status == "creating":
        time.sleep(t)
        if t < 80:
            t *= 2
        session_status = get_session_status(session_id)
        pbar.update(i)
        i += 1
    try:
        pbar.close()
    except:
        pass
    if session_status == "running":
        logging.info(f"session with ID {session_id} is ready")
    elif session_status == "completed":
        raise Exception(f"session with ID {session_id} has already completed")
    elif session_status == "failed":
        raise Exception(f"session with ID {session_id} has failed")
    else:
        raise Exception(f"Unknown session status {session_status} is ready")


def run_session(
    cluster_name=None,
    nworkers=16,
    release_resources_after=20,
    print_logs=False,
    store_logs_in_s3=True,
    store_logs_on_cluster=False,
    sample_rate=None,
    session_name=None,
    files=None,
    code_files=None,
    force_update_files=True,
    pf_dispatch_table=None,
    using_modules=None,
    url=None,
    branch=None,
    directory=None,
    dev_paths=None,
    force_sync=False,
    force_pull=False,
    force_install=False,
    estimate_available_memory=True,
    email_when_ready=None,
    *args,
    **kwargs,
):
    """Starts a session, runs some code files and the sessions ends after that."""

    if sample_rate is None:
        sample_rate = nworkers

    if files is None:
        files = []

    if code_files is None:
        code_files = []

    if using_modules is None:
        using_modules = []

    if dev_paths is None:
        dev_paths = []

    try:
        start_session(
            cluster_name=cluster_name,
            nworkers=nworkers,
            release_resources_after=release_resources_after,
            print_logs=print_logs,
            store_logs_in_s3=store_logs_in_s3,
            store_logs_on_cluster=store_logs_on_cluster,
            sample_rate=sample_rate,
            session_name=session_name,
            files=files,
            code_files=code_files,
            force_update_files=force_update_files,
            pf_dispatch_table=pf_dispatch_table,
            using_modules=using_modules,
            url=url,
            branch=branch,
            directory=directory,
            dev_paths=dev_paths,
            force_sync=force_sync,
            force_pull=force_pull,
            force_install=force_install,
            estimate_available_memory=estimate_available_memory,
            nowait=False,
            email_when_ready=email_when_ready,
            for_running=True,
        )
    except:
        try:
            session_id = get_session_id()
        except:
            session_id = None
        if session_id is not None:
            end_session(get_session_id(), failed=True)
        raise
    finally:
        try:
            session_id = get_session_id()
        except:
            session_id = None
        if session_id is not None:
            end_session(get_session_id(), failed=False)
