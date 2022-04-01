from .config import configure


class Session:
    """Stores information about one session"""

    def __init__(self, cluster_name, session_id, resource_id, nworkers, sample_rate):
        self._cluster_name = cluster_name
        self._session_id = session_id
        self._resource_id = resource_id
        self._nworkers = nworkers
        self._sample_rate = sample_rate
        self._locations = {}
        self._pending_requests = []
        self._futures_on_client = {}

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def resource_id(self):
        return self._resource_id

    @property
    def nworkers(self):
        return self._nworkers

    @property
    def sample_rate(self):
        return self._sample_rate

    # Add other getters if needed


sessions = {}
current_session_id = None


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

    # configure(*args, **kwargs)

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

    # configure(*args, **kwargs)

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

    # configure(*args, **kwargs)

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

    configure(*args, **kwargs)

    return get_session().cluster_name
