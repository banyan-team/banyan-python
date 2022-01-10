from _typeshed import NoneType
import boto3
import random
import time
from typing import Dict, Any, Union
import warnings


def create_cluster(
    name:Union[str,NoneType]=None,
    instance_type:Union[str,NoneType]="m4.4xlarge",
    max_num_workers:Union[int,NoneType]=2048,
    initial_num_workers:Union[int,NoneType]=16,
    min_num_workers:Union[int,NoneType]=0,
    iam_policy_arn:Union[str,NoneType]=None,
    s3_bucket_arn:Union[str,NoneType]=None,
    s3_bucket_name:Union[str,NoneType]=None,
    scaledown_time=25,
    vpc_id=None,
    subnet_id=None,
    nowait=False,
    **kwargs,
):
    clusters = get_clusters(**kwargs) # TODO: where does this function come from?
    if name is None:
        name = "Cluster " + str(len(clusters) + 1)

    # Check if the configuration for this cluster name already exists
    # If it does, then recreate cluster
    if name in clusters:
        if clusters[name].status == "terminated":
            warnings.warn(f"Cluster configuration with {name} already exists. Ignoring new configuration and re-creating cluster.") # TODO: confirm this is what we want
            send_request_get_response("create_cluster", {"cluster_name": name, "recreate":True})
            if not nowait: # TODO: where does this come from?
                wait_for_cluster(name)
            return get_cluster(name)
        else:
            raise RuntimeError(f"Cluster with {name} already exists and has status {str(clusters[name].status)}") # TODO: what kind of error should be raised?
        
    # Construct arguments

    # Construct arguments
    if s3_bucket_name is not None:
        s3_bucket_arn = f"arn:aws:s3:::{s3_bucket_name}"
    elif s3_bucket_arn is not None:
        s3_bucket_name = s3_bucket_arn.split(":")[-1]

    if s3_bucket_arn is None:
        s3_bucket_arn = ""
    elif s3_bucket_name not in s3_list_buckets(get_aws_config()):
        raise ValueError(f"Bucket {s3_bucket_name} does not exist in connected AWS account") # TODO: is this correct type of error

    # Construct cluster creation
    cluster_config = {
        "cluster_name": name,
        "instance_type": instance_type,
        "max_num_workers": max_num_workers,
        "initial_num_workers": initial_num_workers,
        "min_num_workers": min_num_workers,
        "aws_region": get_aws_config_region(),
        "s3_read_write_resource": s3_bucket_arn,
        "scaledown_time": scaledown_time,
        "recreate": False,
    }

    if "ec2_key_pair_name" in c["aws"]:
        cluster_config["ec2_key_pair"] = c["aws"]["ec2_key_pair_name"]
    if iam_policy_arn is not None:
        cluster_config["additional_policy"] = iam_policy_arn
    if vpc_id is not None:
        cluster_config["vpc_id"] = vpc_id
    if subnet_id is not None:
        cluster_config["subnet_id"] = subnet_id

    print("Creating cluster")

    # Send request to create cluster
    send_request_get_response("create_cluster", cluster_config)

    if not nowait:
        wait_for_cluster(name)
    
    return Cluster(name, get_cluster_status(name), "", 0, s3_bucket_arn)

    
def destroy_cluster(name: str, **kwargs):
    configure(**kwargs)
    send_request_get_response("destroy_cluster", Dict[str, Any]({"cluster_name": name}))


def delete_cluster(name: str, **kwargs):
    configure(**kwargs)
    send_request_get_response("destroy_cluster", Dict[str, Any]({"cluster_name": name, "permanently_delete": True}))


def update_cluster(name: str, **kwargs):
    configure(**kwargs)
    send_request_get_response("update_cluster", Dict[str, Any]({"cluster_name": name}))


def assert_cluster_is_ready(name: str, **kwargs):
    print("Setting cluster status to running") # TODO: desired?
    configure(**kwargs)
    send_request_get_response("set_cluster_ready", Dict[str, Any]({"cluster_name": name}))


class Cluster:
    def __init__(self, name: str, status: str, status_explanation: str, num_jobs_running: int, s3_bucket_arn: str):
        self.name = name
        self.status = status
        self.status_explanation = status_explanation
        self.num_jobs_running = num_jobs_running
        self.s3_bucket_arn = s3_bucket_arn

def parsestatus(status):
    accepted = ["creating", "destroying", "updating", "failed", "starting",
    "stopped", "running", "terminated"]
    if status in accepted:
        return status
    else:
        raise ValueError(f"Unexpected status {status}")


def get_clusters(**kwargs):
    configure(**kwargs)
    response = send_request_get_response("describe_clusters", Dict[str, Any]({}))
    return {
        name: Cluster(
            name,
            parsestatus(c["status"]),
            c["status_explanation"] if "status_explanation" in c else "",
            c["num_jobs"],
            c["s3_read_write_resource"],
        ) for (name, c) in response["clusters"]
    }


def get_cluster_s3_bucket_name(cluster_name=None, **kwargs):
    if cluster_name is None:
        cluster_name = get_cluster_name()
    configure(**kwargs)
    cluster = get_cluster(cluster_name)
    return s3_bucket_arn_to_name(cluster.s3_bucket_arn)


def get_cluster(name: str=None, **kwargs):
    if name is None:
        name = get_cluster_name()
        return get_clusters(**kwargs)[name]


def get_running_clusters(*args, **kwargs):
    return [entry for entry in get_clusters(*args, **kwargs) if entry[2].status == "running"]


def get_cluster_status(name:str=None, **kwargs):
    if name is None:
        name = get_cluster_name()
    c = get_clusters(**kwargs)[name]
    if c.status == "failed":
        print(c.status_explanation) # TODO: OK?
    return c.status


def wait_for_cluster(name:str=None, **kwargs):
    if name is None:
        name = get_cluster_name()
    t = 5
    cluster_status = get_cluster_status(name, **kwargs)
    while cluster_status == "creating" or cluster_status == "updating":
        if cluster_status == "creating":
            print(f"Cluster {name} is getting set up")
        else:
            print(f"Cluster {name} is updating")
        time.sleep(t)
        if t < 80:
            t *= 2
        cluster_status = get_cluster_status(name, **kwargs)
    if cluster_status == "running":
        print(f"Cluster {name} is running and ready for jobs")
    elif cluster_status == "terminated":
        print(f"Cluster {name} no longer exists")
    elif cluster_status not in ["creating", "updating"]:
        print(f"Cluster {name} setup has failed")
