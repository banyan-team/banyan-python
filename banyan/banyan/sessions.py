import boto3
import inspect
import logging
import os
from pygit2 import Repository
import time
from tqdm import tqdm
from typing import List

import progressbar

from .constants import BANYAN_PYTHON_BRANCH_NAME, BANYAN_PYTHON_PACKAGES
from .clusters import (
    get_cluster,
    get_running_clusters,
    get_cluster_s3_bucket_name,
    wait_for_cluster,
)
from .config import configure
from .session import (
    get_session_id,
    set_session,
    sessions,
    current_session_id,
    get_session,
    Session,
    start_session_tasks,
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


# Process-local dictionary mapping from session IDs to instances of `Session`
sessions = {}
current_session_id = ""
# Tasks for starting sessions
start_session_tasks = {}


def set_session(session_id: SessionId):
    """Sets the session ID.

    Parameters
    ----------
    session_id : string
        Session ID to use
    """
    global current_session_id
    current_session_id = session_id


def _get_session_id_no_error() -> SessionId:
    global current_session_id
    global sessions
    if current_session_id not in sessions:
        return ""
    return current_session_id


def has_session_id():
    return _get_session_id_no_error() != ""


def get_session_id(session_id="") -> SessionId:
    global current_session_id
    global sessions
    global start_session_tasks
    global session_sampling_configs

    if session_id == "":
        session_id = current_session_id

    if session_id in sessions:
        return session_id
    elif session_id in start_session_tasks:
        start_session_task = start_session_tasks[session_id]
        if istaskdone(start_session_task) and (len(start_session_task.result) == 2):
            e, bt = start_session_task.result
            showerror(stderr, e, bt)
            raise Exception(f"Failed to start session with ID {session_id}")
        elif istaskdone(start_session_task) and (len(start_session_task.result) == 3):
            new_session_id, session, sampling_configs = start_session_task.result
            sessions[new_session_id] = session
            session_sampling_configs[new_session_id] = sampling_configs
            if session_id == current_session_id:
                current_session_id = new_session_id
            return new_session_id
        else:
            # Otherwise, the task is still running or hasn't yet been started
            # in which case we will just return the ID of the start_session task
            return session_id
    elif session_id == "" or session_id is None:
        return start_session()
    elif session_id.startswith("start-session-"):
        raise Exception(
            f"The session with ID {session_id} was not created in this Julia session"
        )
    else:
        return session_id


def get_sessions_dict() -> Dict[SessionId, Session]:
    global sessions
    return sessions


def get_session(session_id=get_session_id(), show_progress=True) -> Session:
    global start_session_tasks
    sessions_dict = get_sessions_dict()
    if session_id in sessions_dict:
        return sessions_dict[session_id]
    elif session_id in start_session_tasks:
        # Schedule the task if not yet scheduled
        start_session_task = start_session_tasks[session_id]
        if not istaskstarted(start_session_task):
            yield start_session_task

        # Keep looping till the task is created

        p = progressbar.ProgressBar(
            [f"Preparing session with ID {session_id}", progressbar.Bar()]
        ).start()
        p_i = 0
        try:
            while get_session_id(session_id) not in get_sessions_dict():
                p.update(p_i + 1)
        except Exception as e:
            p.finish()
            raise
        p.finish()
        return get_sessions_dict()[get_session_id(session_id)]
    else:
        raise Exception(
            f"The session ID {session_id} is not stored as a session starting task in progress or a running session"
        )


def get_cluster_name() -> str:
    """Gets the name of the cluster that the current session is running on.

    Returns
    -------
    string
        Name of the cluster that the current session is running on.
    """

    return get_session().cluster_name


# TO DO - to test this function
def get_loaded_packages():
    """Returns all the packages/libraries that are currently imported by the user"""
    current_session_id = _get_session_id_no_error()
    if current_session_id != "":
        loaded_packages = get_sessions_dict()[current_session_id].loaded_packages
    else:
        loaded_packages = set()
    res = list(loaded_packages)
    res.extend(
        [
            p[0]
            for p in locals().items()
            if inspect.ismodule(p[1]) and not p[0].startswith("__")
        ]
    )
    return res


NOTHING_STRING = "NOTHING_STRING"

StartSessionResult = Tuple[SessionId, Session, Dict[LocationPath, SamplingConfig]]


def _start_session(
    cluster_name: str,
    c: Cluster,
    nworkers: int,
    release_resources_after: int,
    print_logs: bool,
    store_logs_in_s3: bool,
    store_logs_on_cluster: bool,
    log_initialization: bool,
    session_name: str,
    files: List[str],
    code_files: List[str],
    force_update_files: bool,
    pf_dispatch_table: List[str],
    no_pf_dispatch_table: bool,
    using_modules: List[str],
    # We currently can't use modules that require GUI
    not_using_modules: List[str],
    url: str,
    branch: str,
    directory: str,
    dev_paths: List[str],
    force_sync: bool,
    force_pull: bool,
    force_install: bool,
    force_new_pf_dispatch_table: bool,
    estimate_available_memory: bool,
    email_when_ready: bool,
    no_email: bool,
    for_running: bool,
    sessions: Dict[str,Session],
    sampling_configs: Dict[LocationPath,SamplingConfig],
) -> StartSessionResult:
    global session_sampling_configs

    version = get_python_version()

    not_in_modules = lambda m: m not in not_using_modules)
    main_modules = filter(not_in_modules, get_loaded_packages())
    using_modules = filter(not_in_modules, using_modules)
    session_configuration = {
        "cluster_name": cluster_name,
        "num_workers": nworkers,
        "release_resources_after": None if (release_resources_after == -1) else release_resources_after,
        "return_logs": print_logs,
        "store_logs_in_s3": store_logs_in_s3,
        "store_logs_on_cluster": store_logs_on_cluster,
        "log_initialization": log_initialization,
        "version": version,
        "benchmark": os.getenv("BANYAN_BENCHMARK", "0") == "1",
        "main_modules": main_modules,
        "using_modules": using_modules,
        "reuse_resources": not force_update_files,
        "estimate_available_memory": estimate_available_memory,
        "language": "jl",
        "sampling_configs": sampling_configs_to_py(sampling_configs),
        "assume_cluster_is_running": True,
        "force_new_pf_dispatch_table": force_new_pf_dispatch_table,
    }
    if session_name != NOTHING_STRING:
        session_configuration["session_name"] = session_name
    if no no_email:
        session_configuration["email_when_ready"] = email_when_ready
    
    s3_bucket_name = s3_bucket_arn_to_name(c.s3_bucket_arn)
    organization_id = c.organization_id
    curr_cluster_instance_id = c.curr_cluster_instance_id
    
    session_configuration["organization_id"] = organization_id
    session_configuration["curr_cluster_instance_id"] = curr_cluster_instance_id

    environment_info = {}
    # If a url is not provided, then use the local environment
    if url == NOTHING_STRING:

        # TODO: Optimize to not have to send tomls on every call
        local_environment_dir = get_julia_environment_dir()
        project_toml = load_file(f"file://{local_environment_dir}Project.toml")
        if not os.path.isfile(f"{local_environment_dir}Manifest.toml")
            manifest_toml = ""
            logging.warning("Creating a session with a Julia environment that does not have a Manifest.toml")
        else:
            manifest_toml = load_file(f"file://{local_environment_dir}Manifest.toml")
        environment_hash = get_hash(project_toml + manifest_toml + version)
        environment_info["environment_hash"] = environment_hash
        environment_info["project_toml"] = "$(environment_hash)/Project.toml"
        file_already_in_s3 = os.path.isfile(S3Path(f"s3://{s3_bucket_name}/{environment_hash}/Project.toml"))
        if not file_already_in_s3
            s3_put(global_aws_config(), s3_bucket_name, "$(environment_hash)/Project.toml", project_toml)
        if manifest_toml != "":
            environment_info["manifest_toml"] = f"{environment_hash}/Manifest.toml"
            file_already_in_s3 = isfile(S3Path(f"s3://{s3_bucket_name}/{environment_hash}/Manifest.toml", config=global_aws_config()))
            if not file_already_in_s3
                s3_put(global_aws_config(), s3_bucket_name, f"{environment_hash}/Manifest.toml", manifest_toml)
    else:
        # Otherwise, use url and optionally a particular branch
        environment_info["url"] = url
        if directory == NOTHING_STRING:
            raise Exception(f"Directory must be provided for given URL {url}")
        environment_info["directory"] = directory
        if branch != NOTHING_STRING:
            environment_info["branch"] = branch
        environment_info["dev_paths"] = dev_paths
        environment_info["force_pull"] = force_pull
        environment_info["force_install"] = force_install
        environment_info["environment_hash"] = get_hash(
            url + ("" if branch == NOTHING_STRING  else branch end) + "".join(dev_paths)
        )
    environment_info["force_sync"] = force_sync
    session_configuration["environment_info"] = environment_info

    # Upload files to S3
    for f in [*files, *code_files]:
        s3_path = S3Path(f"s3://{s3_bucket_name}/{os.path.basename(f)}", config=global_aws_config())
        if not os.path.isfile(s3_path) or force_update_files
            s3_put(global_aws_config(), s3_bucket_name, basename(f), load_file(f))
        end
    end
    # TODO: Optimize so that we only upload (and download onto cluster) the files if the filename doesn't already exist
    session_configuration["files"] = list(map(os.path.basename, files))
    session_configuration["code_files"] = list(map(os.path.basename, code_files))

    if no_pf_dispatch_table:
        branch_to_use = get_branch_name() if os.getenv("BANYAN_TESTING", "0") == "1" BANYAN_JULIA_BRANCH_NAME
        pf_dispatch_table = []
        for dir in BANYAN_JULIA_PACKAGES:
            pf_dispatch_table.append(f"https://raw.githubusercontent.com/banyan-team/banyan-julia/{branch_to_use}/{dir}/res/pf_dispatch_table.toml")
        end
    end
    session_configuration["pf_dispatch_tables"] = pf_dispatch_table

    # Start the session
    logging.debug("Sending request for start_session")
    response = send_request_get_response("start_session", session_configuration)
    session_id = response["session_id"]
    resource_id = response["resource_id"]
    organization_id = response["organization_id"]
    cluster_instance_id = response["cluster_instance_id"]
    cluster_name = response["cluster_name"]
    reusing_resources = response["reusing_resources"]
    cluster_potentially_not_ready = response["stale_cluster_status"] != "running"
    scatter_queue_url = response["scatter_queue_url"]
    gather_queue_url = response["gather_queue_url"]
    execution_queue_url = response["execution_queue_url"]
    num_sessions = response["num_sessions"]
    num_workers_in_use = response["num_workers_in_use"]
    if for_running:
        msg = "Running session with ID $session_id and $code_files"
    else:
        msg = "Starting session with ID $session_id on cluster named \"$cluster_name\""
    if num_sessions == 0:
        msg += " with no sessions running yet"
    elif num_sessions == 1:
        msg += " with 1 session already running"
    else:
        msg += " with $num_sessions sessions already running"
    if num_workers_in_use > 0:
        if num_sessions == 0:
            msg += " but $num_workers_in_use workers running"
        else:
            msg += " on $num_workers_in_use workers"
    # Store in global state
    new_session = Session(
        cluster_name,
        session_id,
        resource_id,
        nworkers,
        organization_id,
        cluster_instance_id,
        not_using_modules,
        not cluster_potentially_not_ready,
        False,
        scatter_queue_url=scatter_queue_url,
        gather_queue_url=gather_queue_url,
        execution_queue_url=execution_queue_url,
        print_logs=print_logs
    )

    # if !nowait
    wait_for_session(session_id, False)
    # elseif !reusing_resources
    #     @warn "Starting this session requires creating new cloud computing resources which will take 10-30 minutes for the first computation."
    # end

    logging.debug(f"Finished call to start_session with ID {session_id}")
    return session_id, new_session, sampling_configs

def get_session_id(*args, **kwargs):
    """Returns the value of the global variable set to the current session ID.

    Returns
    -------
    string
        Current session ID
    """

    global current_session_id
    if current_session_id is None:
        raise Exception(
            "No session started or selected using `start_session` or `with_session` or `set_session`. The current session may have been destroyed or no session started yet.",
        )
    return current_session_id


def get_session(session_id=None, *args, **kwargs):
    """Get information about the current session.

    Parameter
    --------
    session_id : string
        Session ID to get information for

    Returns
    -------
    Session
        Information about the given session ID

    Raises
    ------
        Exception if the session ID is for a session that wasn't created by this
        process or has failed
    """

    if session_id is None:
        session_id = get_session_id()
    global sessions  # an empty dictionary that will get filled up with mappings from session_id ->instances of the class Session
    if session_id not in sessions:
        raise Exception(
            f"The selected job with ID {session_id} does not have any information; if it was created by this process, it has either failed or been destroyed."
        )
    return sessions[session_id]


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
    pf_dispatch_table: List[str] = None,
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

    c = get_cluster(cluster_name)

    session_configuration = {
        "cluster_name": cluster_name,
        "organization_id": c.organization_id,
        "curr_cluster_instance_id": c.curr_cluster_instance_id,
        "num_workers": nworkers,
        "release_resources_after": release_resources_after,
        "return_logs": print_logs,
        "store_logs_in_s3": store_logs_in_s3,
        "store_logs_on_cluster": store_logs_on_cluster,
        "log_initialization": log_initialization,
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


def print_session_logs(session_id, cluster_name, delete_file=True):
    s3 = boto3.client("s3")
    s3_bucket_name = get_cluster_s3_bucket_name(cluster_name)
    log_file_name = f"banyan-log-for-session-{session_id}"
    try:
        obj = s3.get_object(Bucket=s3_bucket_name, Key=log_file_name)
        print(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"Could not print session logs for session with ID {session_id}")
        print(f"To download session logs, you can use `banyan.download_session_logs()`")
    if delete_file:
        s3.delete_object(Bucket=s3_bucket_name, Key=log_file_name)


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
    project_dir=None,
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

    store_logs_in_s3_orig = store_logs_in_s3

    try:
        if print_logs:
            # If logs need to be printed, ensure that we save logs in S3. If
            # store_logs_in_s3==False, then delete logs in S3 later
            store_logs_in_s3 = True
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
            project_dir=project_dir,
            url=url,
            branch=branch,
            directory=directory,
            dev_paths=dev_paths,
            force_sync=force_sync,
            force_pull=force_pull,
            force_install=force_install,
            estimate_available_memory=estimate_available_memory,
            nowait=False,  # Wait untile session is ready, since code files are running
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
            if print_logs:
                print_session_logs(
                    session_id, cluster_name, delete_file=(not store_logs_in_s3_orig)
                )
        raise
    finally:
        try:
            session_id = get_session_id()
        except:
            session_id = None
        if session_id is not None:
            end_session(get_session_id(), failed=False)
            if print_logs:
                print_session_logs(
                    session_id, cluster_name, delete_file=(not store_logs_in_s3_orig)
                )
