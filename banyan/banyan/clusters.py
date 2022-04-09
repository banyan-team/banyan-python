from tqdm import tqdm
import urllib

from .config import configure
from .imports import *
from .session import get_cluster_name
from .utils import (
    send_request_get_response,
    get_aws_config_region,
    s3_bucket_arn_to_name,
)


s3 = boto3.client("s3")
clusters = dict()


def create_cluster(
    name: str = None,
    instance_type: str = "m4.4xlarge",
    max_num_workers: str = 2048,
    initial_num_workers: int = 16,
    min_num_workers: int = 0,
    iam_policy_arn: str = None,
    s3_bucket_arn: str = None,
    s3_bucket_name: str = None,
    scaledown_time: int = 25,
    region: str = None,
    vpc_id: str = None,
    subnet_id: str = None,
    nowait: bool = False,
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

    clusters_local = get_clusters(**kwargs)
    if name is None:
        name = "Cluster " + str(len(clusters_local) + 1)
    if region is None:
        region = get_aws_config_region()

    # Check if the configuration for this cluster name already exists
    # If it does, then recreate cluster
    if name in clusters_local:
        if clusters_local[name].status == "terminated":
            logging.info(f"Started re-creating cluster named {name}")
            send_request_get_response(
                "create-cluster", {"cluster_name": name, "recreate": True}
            )
            if not nowait:
                wait_for_cluster(name)
            # Cache info
            return get_cluster(name)
        else:
            raise Exception(
                f"Cluster with name {name} already exists and its current status is {str(clusters_local[name].status)}"
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
        "cluster_name": name,
        "instance_type": instance_type,
        "max_num_workers": max_num_workers,
        "initial_num_workers": initial_num_workers,
        "min_num_workers": min_num_workers,
        "aws_region": region,
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
    if not ec2_key_pair == None:
        cluster_config["ec2_key_pair"] = ec2_key_pair

    logging.info(f"Started creating cluster named {name}")
    # Send request to create cluster
    send_request_get_response("create-cluster", cluster_config)

    if not nowait:
        wait_for_cluster(name)

    # Cache info
    get_cluster(name)

    return clusters[name]


def destroy_cluster(name: str, **kwargs):
    configure(**kwargs)
    logging.info(f"Destroying cluster named {name}")
    send_request_get_response("destroy-cluster", {"cluster_name": name})


def delete_cluster(name: str, **kwargs):
    print("1")
    configure(**kwargs)
    print("2")
    logging.info(f"Deleting cluster named {name}")
    print("3")
    send_request_get_response(
        "destroy-scluster", {"cluster_name": name, "permanently_delete": True}
    )
    print("4")


def update_cluster(name: str, **kwargs):
    configure(**kwargs)
    logging.info(f"Updating cluster named {name}")
    send_request_get_response("update-cluster", {"cluster_name": name})


def assert_cluster_is_ready(name: str, **kwargs):
    logging.info(f"Setting status of cluster named {name} to running")
    configure(**kwargs)
    send_request_get_response("set-cluster-ready", {"cluster_name": name})


class Cluster:
    def __init__(
        self, name: str, status: str, status_explanation: str, s3_bucket_arn: str
    ):
        self.name = name
        self.status = status
        self.status_explanation = status_explanation
        self.s3_bucket_arn = s3_bucket_arn


def get_clusters(cluster_name=None, **kwargs):
    logging.debug("Downloading description of clusters")
    print(configure(**kwargs))
    filters = {}
    if cluster_name is not None:
        filters["cluster_name"] = cluster_name
    response = send_request_get_response("describe-clusters", {"filters": filters})
    clusters_dict = {
        name: Cluster(
            name,
            c["status"],
            c["status_explanation"] if "status_explanation" in c else "",
            c["s3_read_write_resource"],
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
