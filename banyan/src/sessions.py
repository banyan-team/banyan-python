import code
import logging
import os
import boto3
import time

from pytz import NonExistentTimeError
from .session import get_session_id, set_session, sessions, current_session_id, get_session

from .clusters import (
    get_running_clusters, 
    get_cluster_s3_bucket_name, 
    wait_for_cluster
)
from .config import configure
#from jobs import get_job_id
from .session import Session
from .utils import (
    send_request_get_response,
    get_loaded_packages,
    get_python_version,
    get_hash,
    upload_file_to_s3,
    load_toml
)



def end_session(session_id = None, failed = False, release_resources_now = False, release_resources_after = None, *args, **kwargs):
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

    #Ending session with ID session_ID
    request_params = {'session_id': session_id, 'failed': failed , 'release_resources_now': release_resources_now}
    if release_resources_after is not None:
        request_params['release_resources_after'] = release_resources_after
    send_request_get_response(
        'end_session', 
        request_params   
    )

    #Remove from global state
    set_session(None)
    del sessions[session_id]
    return session_id
 
def end_all_sessions(cluster_name, release_resources_now = False, release_resources_after = None, *args, **kwargs):
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
    
    sessions = get_sessions(cluster_name, status = ['creating', 'running'])
    for (session_id, session) in sessions.items():
        end_session(session_id, release_resources_now, release_resources_after)

def get_sessions(cluster_name = None, status = None, *args, **kwargs):
    """Gets information about all the sessions for the user. Optionally can filter
    by cluster name and status. 
        
    Parameters
    ----------
    clusterr_name : string 
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
        filters['cluster_name'] = cluster_name #mapping from key 'cluster_name' -> actual value of the parameter cluster_name
    
    if status is not None:
        filters['status'] = status

    #The function repeatedly calls the send_request_get_response function that takes 
    #the string 'describe_sessions' and the dictionary that contains filters. looping until 
    #'last_eval' does not exist in the indiv_response dictionary.  
    
    sessions = {} #empty dictionary
    indiv_response = send_request_get_response('describe_sessions', {'filters':filters})["session"]
    sessions.update(indiv_response['sessions'])
    while 'last_eval' in indiv_response:
        current_last_eval = indiv_response['last_eval']
        indiv_response = send_request_get_response('describe_sessions', {'filters':filters, 'this_start_key':current_last_eval})
        sessions.update(indiv_response['sessions'])

    # sessions is a dictionary that contains
    # mappings from session_id to another dictionary containing information about the session
    #                             {"start_time": "0126202220937124", "end_time": "", "num_workers": 2}
    # both start_time and end_time are of the format "yyyy-mm-dd-HH:MM:SSzzzz"
    for id, s in sessions.items(): #iterating over key and value
        if sessions[id]['end_time'] == '':
            sessions[id]['end_time'] = None
        else: 
            sessions[id]['end_time'] = parse_time(sessions[id]['end_time'])
        sessions[id]['start_time'] = parse_time(sessions[id]['start_time'])
    return sessions

def get_running_sessions(*args, **kwargs):
    """Gets info about all sessions that are currently running 
    
    Returns
    -------
    Dictionary
        Mappings from session ID to another dictionary containing information about sessions that are running
    """
    
    configure(*args, **kwargs)
    
    return get_sessions(status = 'running')

def get_session_status(session_id = None, *args, **kwargs):
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
    filters = {'session_id':session_id} #filters is a dictionary 
    # response contains a dictionary (at the key "sessions") which maps from session IDs
    # to session information where the session information is a dictionary containing
    # various info such as the status (at the key "status").  
    response = send_request_get_response('describe_sessions', {'filters':filters})
    session_status = response['sessions'][session_id]['status']
    if session_status == 'failed':
        # We don't immediately fail - we're just explaining. It's only later on
        # where it's like we're actually using this session do we set the status.
        logging.info(response['status_explanation']) 
    return session_status

#>>> response = {'sessions': {}} response contains a dictionary at the key 'sessions' 
#>>> response = {'sessions': {'19876' : {'name' :'my_session'}}}

#>>> response['sessions']
#{'19876': {'name': 'my_session'}}

#>>> response['sessions']['19876']['name']
#'my_session'

#>>> response['sessions']['0']= {'name' :'my_session'}
#>>> response
#{'sessions': {'19876': {'name': 'my_session'}, '0': {'name': 'my_session'}}}
#>>> d={}
#>>> d[500] = "hello"
#>>> d
#{500: 'hello'}
#>>> d.update({50: "hello th", 100: "he"})
#>>> d
#{500: 'hello', 50: 'hello th', 100: 'he'}

def download_session_logs(session_id, cluster_name, filename = None, *args, **kwargs):
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
    log_file_name = f"banyan-log-for-session-{session_id}" #This creates a string with the {session_id} replaced with the value of the job_id
    if filename is None: # if fileneame is not specified
        filename = os.path.expanduser("~") # path to the home directory on the computer
    s3 = boto3.client('s3')
    s3.download_file(s3_bucket_name, log_file_name, filename)

def wait_for_session(session_id = None, *args, **kwargs):
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
    session_status = get_session_status (session_id)
    t = 5
    while session_status == 'creating':
        time.sleep(t)
        if t < 80:
            t *= 2
        session_status = get_session_status(session_id)
    if session_status == 'running':
        logging.info(f"session with ID {session_id} is ready")
    elif session_status == 'completed':
        raise Exception(f"session with ID {session_id} has already completed")
    elif session_status == 'failed':
        raise Exception(f"session with ID {session_id} has failed")
    else:
        raise Exception(f"Unknown session status {session_status} is ready")

def with_session(f, *args, **kwargs):
    """
    """
    
    configure(*args, **kwargs)

    use_exising_session = 'session' in kwargs.keys()
    end_session_on_error = kwargs.get('end_on_session_error', True)
    end_session_on_exit = kwargs.get('end_session_on_exit', True)

    pass


def start_session(
    cluster_name = None,
    nworkers = 16,
    release_resources_after = 20,
    print_logs = False,
    store_logs_in_s3 = True,
    store_logs_on_cluster = False,
    sample_rate = None,
    session_name = None,
    files = None,
    code_files = None,
    force_update_files = False,
    pf_dispatch_table = "",
    using_modules = None,
    # pip_requirements_file = None, # paths to a requirements.txt file that contains packages to be installed with pip
    # conda_environment_file = None, # paths to environment.yml file that contains packages to be installed with conda
    project_dir = None, # a pyproject.toml file containing information about a poetry environment
    url = None,
    branch = None,
    directory = None,
    dev_paths = None,
    force_clone = False,
    force_pull = False,
    force_install = False,
    estimate_available_memory = True,
    nowait = False,
    email_when_ready = None,
    *args, **kwargs,  
):
    """
    Starts a new session.

    Parameters
    ----------

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
        project_dir = os.getcwd() #gets teh current working directory
        
    poetry_pyproject_file = os.path.join(project_dir, "pyproject.toml")
    poetry_lock_file = os.path.join(project_dir, "poetry.lock")

    # Configure
    #TO DO

    # Construct parameters for starting session
    if cluster_name is None:
        # running_clusters is dictionary
        running_clusters = get_running_clusters()
        if len(running_clusters) == 0:
            raise Exception("Failed to start session: you don't have any clusters created")
        else:
            cluster_name = list(running_clusters.keys())[0]

    version = get_python_version()
    
    session_configuration = {
        'cluster_name': cluster_name,
        'num_workers': nworkers,
        'release_resources_after':release_resources_after,
        'return_logs': print_logs,
        'store_logs_in_s3': store_logs_in_s3,
        'store_logs_on_cluster': store_logs_on_cluster,
        'version': version,
        # ENV is a dictionary in Julia that contains environment variables
        # To get the value of an environment variable in python (os)
        'benchmark': os.environ.get('BANYAN_BENCHMARK', '0') == '1', #comparison
        'main_modules': get_loaded_packages(),
        'using_modules': using_modules,
        'reuse_resources': not force_update_files,
        'estimate_available_memory': estimate_available_memory,    
        'language' : 'py'
    }
   
    if session_name is None:
        session_configuration['session_name'] = session_name
    
    if email_when_ready is None:
        session_configuration['email_when_ready'] = email_when_ready
     
    s3_bucket_name = get_cluster_s3_bucket_name(cluster_name)

    environment_info ={}
    # If a url is not provided, then use the local environment
    if url is None:
        #TO DO 
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
            environment_info['environment_hash'] = environment_hash

            # Upload the pyproject.toml file to S3
            object_name = environment_hash + '/pyproject.toml'
            upload_file_to_s3(poetry_pyproject_file, s3_bucket_name, object_name)
            environment_info['pyproject_toml'] = object_name

            if poetry_lock_file_contents != "":
                object_name = environment_hash + '/poetry.lock'
                upload_file_to_s3(poetry_lock_file, s3_bucket_name, object_name)
                environment_info['poetry_lock'] = object_name
                
        else:
            # It has to exist!
            raise Exception("poetry_pyproject_file does not exist")

    else:
    # Otherwise, use url and optionally a particular branch
        environment_info['url'] = url
    
        if directory is None:
            raise Exception("Directory must be provided for given URL $url")
        
        environment_info['directory'] = directory
    
        if branch is None:
            environment_info['branch'] = branch
    
        environment_info['dev_paths'] = dev_paths
        environment_info['force_clone'] = force_clone
        environment_info['force_pull'] = force_pull
        environment_info['force_install'] = force_install
        environment_info['environment_hash'] = get_hash(
            url + ("" if branch is None else branch)
        )

    session_configuration['environment_info'] = environment_info
    
    # Upload files to S3      
    #TO DO upload files and encode files to S3
    for f in files:
        upload_file_to_s3(f.replace('file://', ''), s3_bucket_name)
    for f in code_files:
        upload_file_to_s3(f.replace('file://', ''), s3_bucket_name)
    # TO DO optimize by not uploading if the file exists in S3    

    # Example of f might be "C:/Users/Melany Winston/.../src/clusters.py" --> "cluster.py" extracting out cluster.py
    session_configuration['files'] = [os.path.basename(f) for f in files]
    session_configuration['code_files'] = [os.path.basename(f) for f in code_files]

    #if pf_dispatch_table is None:
        #TO DO add this
    #    pass

    pf_dispatch_table_loaded = {} #load_toml(pf_dispatch_table)
    session_configuration['pf_dispatch_table'] = pf_dispatch_table_loaded
    session_configuration['language'] = 'py' 

    # Start the session
    response = send_request_get_response('start_session', session_configuration)
    session_id = response['session_id']
    resource_id = response['resource_id']

# Store in global state
    set_session(current_session_id, Session(cluster_name, current_session_id, resource_id, nworkers, sample_rate))

    wait_for_cluster(cluster_name)

    if not nowait:
        wait_for_session(session_id)

    return session_id

def run_session(
    cluster_name = None,
    nworkers = 16,
    release_resources_after = 20,
    print_logs = False,
    store_logs_in_s3 = True,
    store_logs_on_cluster = False,
    sample_rate = None,
    session_name = None,
    files = None, 
    code_files = None,
    force_update_files = True,
    pf_dispatch_table = None,
    using_modules = None, 
    url = None, 
    branch = None,
    directory = None,
    dev_paths = None,
    force_clone = False,
    force_pull = False,
    force_install = False,
    estimate_available_memory = True,
    email_when_ready = None,
    *args, **kwargs 
):
    """Starts a session, runs some code files and the sessions ends after that.  
    """

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
        start_session(cluster_name = cluster_name, nworkers = nworkers, release_resources_after = release_resources_after, 
                    print_logs = print_logs, store_logs_in_s3 = store_logs_in_s3, store_logs_on_cluster = store_logs_on_cluster, 
                    sample_rate = sample_rate, session_name = session_name, files = files, code_files = code_files, force_update_files = force_update_files,
                    pf_dispatch_table = pf_dispatch_table, using_modules = using_modules, url = url, branch = branch,
                    directory = directory, dev_paths = dev_paths, force_clone = force_clone, force_pull = force_pull, force_install = force_install, 
                    estimate_available_memory = estimate_available_memory, nowait = False, email_when_ready = email_when_ready, for_running = True)
    except :
        try:
            session_id = get_session_id()
        except:
            session_id = None    
        if session_id is not None:
            end_session(get_session(), failed = True)
        raise
    finally:
        try:
            session_id = get_session_id()
        except:
            session_id = None
        if session_id is not None:
            end_session(get_session_id(), failed = False)
    

