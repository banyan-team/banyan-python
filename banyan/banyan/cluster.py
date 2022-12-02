class Cluster:
    def __init__(
        self,
        cluster_name: str,
        status: str,
        status_explanation: str,
        s3_bucket_arn: str,
        organization_id: str,
        curr_cluster_instance_id: str,
        num_sessions_running: int,
        num_workers_running: int,
    ):
        self.cluster_name = cluster_name
        self.status = status
        self.status_explanation = status_explanation
        self.s3_bucket_arn = s3_bucket_arn
        self.organization_id = organization_id
        self.curr_cluster_instance_id = curr_cluster_instance_id
        self.num_sessions_running = num_sessions_running
        self.num_workers_running = num_workers_running
