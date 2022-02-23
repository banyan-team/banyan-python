from .config import configure

class Session:
    """Stores information about one session
    """
    def _init_(self, cluster_name, session_id, resource_id, nworkers, sample_rate):
        self.cluster_name = cluster_name
        self.session_id = session_id
        self.resource_id = resource_id
        self.nworkers = nworkers
        self.sample_rate = sample_rate
        self.locations = {}
        self.pending_requests = []
        self.futures_on_client = {}

    @property
    def cluster_name(self):
        return self.cluster_name
    
    @property
    def job_id(self):
        return self.job_id
    
    @property
    def nworkers(self):
        return self.nworkers

    @property
    def sample_rate(self):
        return self.sample_rate
 
sessions = {}
current_session_id = None

def set_session(session_id:str, session=None, *args, **kwargs):
    """Sets the session ID.

    Parameters
    ----------
    session_id : string
        Session ID to use
    session : Session
        If not None (default), the global sessions table is updated to include
        this session.
    """ 
    
    configure(args, kwargs)
    
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
    
    configure(args, kwargs)
    
    global current_session_id
    return current_session_id

def get_session(session_id = None, *args, **kwargs):
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
    
    configure(args, kwargs)
    
    if session_id is None:
        session_id = get_session_id()
    global sessions # an empty dictionary that will get filled up with mappings from session_id ->instances of the class Session
    if (session_id not in sessions):
        raise Exception(f"The selected job with ID {session_id} does not have any information; if it was created by this process, it has either failed or been destroyed.")
    return sessions[session_id]

def get_cluster_name(*args, **kwargs):
    """Gets the name of the cluster that the current session is running on.

    Returns
    -------
    string
        Name of the cluster that the current session is running on.
    """
    
    configure(args, kwargs)
    
    return get_session().cluster_name
