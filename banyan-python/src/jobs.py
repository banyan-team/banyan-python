# jobs = Dict()
# current_job_id = None


# def set_job(job_id:Union[JobId, None]):
#     global current_job_id
#     current_job_id = job_id

# def get_job_id()->JobId:
#     global current_job_id
#     if current_job_id is None:
#         logging.error("No job selected using `create_job` or `with_job` or `set_job`. The current job may have been destroyed or no job created yet.")
#     return current_job_id

# def get_job(job_id)->Job:
#     if job_id is None:
#         job_id = get_job_id()
#     global jobs
#     if job_id not in jobs:
#         logging.error("The selected job does not have any information; if it was created by this process, it has either failed or been destroyed.")
#     return jobs[job_id]

# def get_cluster_name():
#     return get_job().cluster_name
