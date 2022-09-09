from typing import List

from .constants import NOT_USING_MODULES
from .id import ResourceId, SessionId


sessions = {}
current_session_id = None


class Session:
    """Stores information about one session"""

    def __init__(
        self,
        cluster_name: str,
        session_id: SessionId,
        resource_id: ResourceId,
        nworkers: int,
        sample_rate: int,
        organization_id: str = "",
        cluster_instance_id: str = "",
        not_using_modules: List[str] = NOT_USING_MODULES,
        is_cluster_ready: bool = False,
        is_session_ready: bool = False,
        scatter_queue_url: str = "",
        gather_queue_url: str = "",
        execution_queue_url: str = "",
    ):
        self.session_id = session_id
        self.resource_id = resource_id
        self.nworkers = nworkers
        self.sample_rate = sample_rate
        self.locations = {}
        self.pending_requests = []
        self.futures_on_client = {}
        self.cluster_name = cluster_name
        self.worker_memory_used = 0
        self.organization_id = organization_id
        self.cluster_instance_id = cluster_instance_id
        self.not_using_modules: List[str] = not_using_modules
        self.loaded_packages: set[str] = set()
        self.is_cluster_ready = is_cluster_ready
        self.is_session_ready = is_session_ready
        self.scatter_queue_url = scatter_queue_url
        self.gather_queue_url = gather_queue_url
        self.execution_queue_url = execution_queue_url


def set_session(session_id: str, session=None, *args, **kwargs):
    """Sets the session ID.

    Parameters
    ----------
    session_id : string
        Session ID to use
    session : Session
        If not None (default), the global sessions table is updated to include
        this session.
    """

    global current_session_id
    current_session_id = session_id

    global sessions
    if session is not None:
        sessions[current_session_id] = session


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


def get_cluster_name(*args, **kwargs):
    """Gets the name of the cluster that the current session is running on.

    Returns
    -------
    string
        Name of the cluster that the current session is running on.
    """

    return get_session().cluster_name
