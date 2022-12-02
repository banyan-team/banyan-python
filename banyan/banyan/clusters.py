from asyncio import wait_for
import logging
from math import ceil
import os
from tqdm import tqdm
import urllib

import boto3

from .cluster import Cluster
from .config import configure
from .session import get_cluster_name
from .utils import (
    send_request_get_response,
    get_aws_config_region,
    s3_bucket_arn_to_name,
    parse_bytes,
)


s3 = boto3.client("s3")
clusters = dict()


def create_cluster(
    cluster_name: str = None,
    instance_type: str = "m4.4xlarge",
    max_num_workers: str = 2048,
    initial_num_workers: int = 16,
    min_num_workers: int = 0,
    iam_policy_arn: str = None,
    s3_bucket_arn: str = None,
    s3_bucket_name: str = None,
    disk_capacity="1200 GiB",  # some # of GBs or "auto" to use Amazon EFS
    scaledown_time: int = 25,
    region: str = None,
    vpc_id: str = None,
    subnet_id: str = None,
    wait_now: bool = True,
    force_create: bool = False,
    destroy_cluster_after=-1,
    show_progress=True,
    ec2_key_pair: str = None,
    **kwargs,
):
    """Creates a new cluster or re-creates a previously destroyed cluster.
    If no vpc_id and subnet_id are provided,
    the cluster is by default created in the default public VPC
    and subnet in your AWS account.
    """
    global clusters

    # Configure using parameters
    c = configure(**kwargs)

    clusters_local = get_clusters(cluster_name, kwargs)

    if cluster_name is None:
        cluster_name = "cluster-" + str(len(clusters_local) + 1)
    if region is None:
        region = get_aws_config_region()

    # Check if the configuration for this cluster name already exists
    # If it does, then recreate cluster
    if cluster_name in clusters_local:
        if force_create or (clusters_local[cluster_name].status == "terminated"):
            if show_progress:
                logging.info(f"Started re-creating cluster named {cluster_name}")
            send_request_get_response(
                "create-cluster",
                {
                    "cluster_name": cluster_name,
                    "recreate": True,
                    "force_create": True,
                    "destroy_cluster_after": destroy_cluster_after,
                },
            )
            if wait_now:
                wait_for_cluster(cluster_name, kwargs)
            # Cache info
            return get_cluster(cluster_name, kwargs)
        elif clusters_local[cluster_name].status == "creating":
            if wait_now:
                wait_for_cluster(cluster_name, kwargs)
            return get_cluster(cluster_name, kwargs)
        else:
            raise Exception(
                f"Cluster with name {cluster_name} already exists and its current status is {str(clusters_local[cluster_name].status)}"
            )

    # Construct arguments
    if s3_bucket_name is not None:
        s3_bucket_arn = f"arn:aws:s3:::{s3_bucket_name}"
    elif s3_bucket_arn is not None:
        s3_bucket_name = s3_bucket_arn.split(":")[-1]

    if s3_bucket_arn is None:
        s3_bucket_arn = ""
    elif s3_bucket_name not in s3.list_buckets():
        logging.error(
            f"Bucket {s3_bucket_name} does not exist in connected AWS account"
        )

    # Construct cluster creation
    cluster_config = {
        "cluster_name": cluster_name,
        "instance_type": instance_type,
        "max_num_workers": max_num_workers,
        "initial_num_workers": initial_num_workers,
        "min_num_workers": min_num_workers,
        "aws_region": region,
        "s3_read_write_resource": s3_bucket_arn,
        "scaledown_time": scaledown_time,
        "recreate": False,
        # We need to pass in the disk capacity in # of GiB and we do this by dividing the input
        # by size of 1 GiB and then round up. Then the backend will determine how to adjust the
        # disk capacity to an allowable increment (e.g., 1200 GiB or an increment of 2400 GiB
        # for AWS FSx Lustre filesystems)
        "disk_capacity": -1
        if (disk_capacity == "auto")
        else ceil(parse_bytes(disk_capacity) / 1.073741824e7),
        "destroy_cluster_after": destroy_cluster_after,
    }

    if "ec2_key_pair_name" in c["aws"]:
        cluster_config["ec2_key_pair"] = c["aws"]["ec2_key_pair_name"]
    if iam_policy_arn is not None:
        cluster_config["additional_policy"] = iam_policy_arn
    if vpc_id is not None:
        cluster_config["vpc_id"] = vpc_id
    if subnet_id is not None:
        cluster_config["subnet_id"] = subnet_id
    if not ec2_key_pair == None:
        cluster_config["ec2_key_pair"] = ec2_key_pair

    if show_progress:
        logging.info(f"Started creating cluster named {cluster_name}")
    # Send request to create cluster
    send_request_get_response("create-cluster", cluster_config)

    if wait_now:
        wait_for_cluster(cluster_name)

    # Cache info
    get_cluster(cluster_name, kwargs)

    return clusters[cluster_name]


def destroy_cluster(name: str, **kwargs):
    configure(**kwargs)
    logging.info(f"Destroying cluster named {name}")
    send_request_get_response("destroy-cluster", {"cluster_name": name})


def delete_cluster(name: str, **kwargs):
    configure(**kwargs)
    logging.info(f"Deleting cluster named {name}")
    send_request_get_response(
        "destroy-cluster", {"cluster_name": name, "permanently_delete": True}
    )


def update_cluster(name: str, **kwargs):
    configure(**kwargs)
    logging.info(f"Updating cluster named {name}")
    send_request_get_response("update-cluster", {"cluster_name": name})


def assert_cluster_is_ready(name: str, **kwargs):
    logging.info(f"Setting status of cluster named {name} to running")
    configure(**kwargs)
    send_request_get_response("set-cluster-ready", {"cluster_name": name})


def get_clusters(cluster_name=None, **kwargs):
    logging.debug("Downloading description of clusters")
    filters = {}
    if cluster_name is not None:
        filters["cluster_name"] = cluster_name
    response = send_request_get_response("describe-clusters", {"filters": filters})
    clusters_dict = {
        name: Cluster(
            name,
            c["status"],
            c.get("status_explanation", ""),
            c["s3_read_write_resource"],
            c["organization_id"],
            c.get("curr_cluster_instance_id", ""),
            c.get("num_sessions", 0),
            c.get("num_workers_in_use", 0),
        )
        for (name, c) in response["clusters"].items()
    }

    # Cache info
    global clusters
    for (name, c) in clusters_dict.items():
        clusters[name] = c

    return clusters_dict


def get_cluster_s3_bucket_arn(cluster_name=None, **kwargs):
    configure(**kwargs)
    if cluster_name is None:
        cluster_name = get_cluster_name()
    global clusters
    # Check if cached, since this property is immutable
    if cluster_name not in clusters:
        get_cluster(cluster_name)
    return clusters[cluster_name].s3_bucket_arn


def get_cluster_s3_bucket_name(cluster_name=None, **kwargs):
    configure(**kwargs)
    if cluster_name is None:
        cluster_name = get_cluster_name()
    return s3_bucket_arn_to_name(get_cluster_s3_bucket_arn(cluster_name))


def get_cluster(name: str = None, **kwargs):
    if name is None:
        name = get_cluster_name()
    return get_clusters(name, **kwargs)[name]


def get_running_clusters(*args, **kwargs):
    return dict(
        filter(
            lambda entry: entry[1].status == "running",
            get_clusters(*args, **kwargs).items(),
        )
    )


def get_cluster_status(name: str = None, **kwargs):
    if name is None:
        name = get_cluster_name()
    global clusters
    if name in clusters:
        if clusters[name].status == "failed":
            logging.error(clusters[name].status_explanation)
    # If it is not failed, then retrieve status, in case it has changed
    c = get_cluster(name, **kwargs)
    if c.status == "failed":
        raise Exception(c.status_explanation)
    return c.status


def wait_for_cluster(name: str = None, **kwargs):
    if name is None:
        name = get_cluster_name()
    t = 5
    i = 0
    cluster_status = get_cluster_status(name, **kwargs)
    while cluster_status == "creating" or cluster_status == "updating":
        if cluster_status == "creating":
            pbar = tqdm(desc=f"Setting up cluster {name}")
        else:
            pbar = tqdm(desc=f"Updating cluster {name}")
        time.sleep(t)
        if t < 80:
            t *= 2
        cluster_status = get_cluster_status(name, **kwargs)
        pbar.update(i)
        i += 1
    try:
        pbar.close()
    except:
        pass
    if cluster_status == "running":
        logging.info(f"Cluster {name} is ready")
    elif cluster_status == "terminated":
        raise Exception(f"Cluster {name} no longer exists")
    elif cluster_status not in ["creating", "updating"]:
        raise Exception(f"Failed to set up cluster named {name}")
    else:
        raise Exception(f"Cluster {name} has unexpected status: {cluster_status}")


def upload_to_s3(src_path, dst_name=None, cluster_name=None, **kwargs):
    if dst_name is None:
        dst_name = os.path.basename(src_path)
    if cluster_name is None:
        cluster_name = get_cluster_name()

    configure(**kwargs)
    bucket_name = get_cluster_s3_bucket_name(cluster_name)

    if src_path.startswith("http://") or src_path.startswith("https://"):
        with urllib.request.urlopen(src_path) as f:
            s3.upload_fileobj(f, bucket_name, dst_name)
    if src_path.startswith("s3://"):
        bucket_src, key_src = src_path.replace("s3://", "").split("/", 1)
        copy_source = {"Bucket": bucket_src, "Key": key_src}
        s3.meta_client.copy(copy_source, bucket_name, dst_name)
    else:
        if src_path.startswith("file://"):
            src_path = src_path[8:]
        src_path = Path(src_path)

        if src_path.is_file():
            s3.meta.client.upload_file(src_path, bucket_name, dst_name)
        else:
            files = [
                f
                for f in os.listdir(src_path)
                if os.path.isfile(os.path.join(src_path, f))
            ]
            for file in files:
                s3.meta.client.upload_file(
                    Path(os.path.join(src_path, file)),
                    bucket_name,
                    os.path.join(dst_name, file),
                )
    return dst_name
