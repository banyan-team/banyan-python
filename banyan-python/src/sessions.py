from jobs import get_job_id
from utils import send_request_get_response
import logging
from clusters import get_cluster_s3_bucket_name
import os
import boto3
import time

def end_all_sessions(cluster_name):
    sessions = get_sessions(cluster_name, status = ['creating', 'running'])
    for (job_id, session) in sessions.items():
        if sessions['status'] =='running':
            end_session(job_id)

def end_session(job_id = get_job_id(), failed = False):
    send_request_get_response(
        'end_session', 
        {'job_id' : job_id , 'failed' : failed}
    )

def get_sessions(cluster_name = None, status = None):
    filters = {}
    if cluster_name is not None:
        filters['cluster_name'] = cluster_name
    
    if status is not None:
        filters['status'] = status
    
    sessions = {}
    indiv_response = send_request_get_response('describe_sessions', {'filters':filters})
    while 'last_eval' in indiv_response:
        current_last_eval = indiv_response['last_eval']
        indiv_response = send_request_get_response('describe_sessions', {'filters':filters, 'this_start_key':current_last_eval})
        sessions.update(indiv_response)  
    return sessions  

def get_running_sessions():
    get_sessions(status = 'running')
    return get_sessions

def get_session_status(job_id = get_job_id()):
    filters = {'job_id':job_id}
    response = send_request_get_response('describe_sessions', {'filters':filters})
    session_status = response['sessions'][job_id]['status']
    if session_status == 'failed':
        logging.info(response['status_explanation']) 
    return session_status

def download_session_logs(job_id, cluster_name, filename = None):
    s3_bucket_name = get_cluster_s3_bucket_name(cluster_name)
    log_file_name = f"banyan-log-for-job-{job_id}"
    if filename is None:
        filename = os.path.expanduser("~")
    s3 = boto3.client('s3')
    s3.download_file(s3_bucket_name, log_file_name, filename)

def wait_for_session(job_id = get_job_id()):
    session_status = get_session_status (job_id)
    t = 5
    while session_status is 'creating':
        time.sleep(t)
        if t < 80:
            t *= 2
        session_status = get_session_status(job_id)
    if session_status is 'running':
        logging.info(f"session with ID {job_id} is ready")
    elif session_status is 'completed':
        raise Exception(f"session with ID {job_id} has already completed")
    elif session_status is 'failed':
        raise Exception(f"session with ID {job_id} has failed")
    else:
        raise Exception(f"Unknown session status {session_status} is ready")

def with_session():
    pass

def create_session():
    pass

