from .config import configure
from .imports import *
from .session import get_cluster_name
from .utils import send_request_get_response, get_aws_config_region

import urllib

# __all__ = [
#     "create_cluster",
#     "destroy_cluster",
#     "delete_cluster",
#     "update_cluster",
#     "assert_cluster_is_ready",
#     "Cluster"
# ]


s3 = boto3.client('s3')
clusters = dict()

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
    region=None,
    vpc_id=None,
    subnet_id=None,
    nowait=False,
    ec2_key_pair = None,
    **kwargs,
):
    """Creates a new cluster or re-creates a previously destroyed cluster.
    If no vpc_id and subnet_id are provided, 
    the cluster is by default created in the default public VPC 
    and subnet in your AWS account.
    The name of the AWS EC2 Key Pair that will be used 
    to SSH into clustermust be provided.
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
                "create_cluster",
                Dict(cluster_name=name, recreate=True)
            )
            if not nowait:
                wait_for_cluster(name)
            return get_cluster(name)
        else:
            logging.error(f"Cluster with name {name} already exists and its current status is {str(clusters_local[name].status)}") 

    # Construct arguments
    if s3_bucket_name is not None:
        s3_bucket_arn = f"arn:aws:s3:::{s3_bucket_name}"
    elif s3_bucket_arn is not None:
        s3_bucket_name = s3_bucket_arn.split(":")[-1]

    if s3_bucket_arn is None:
        s3_bucket_arn = ""
    elif s3_bucket_name not in s3.list_buckets():
        logging.error(f"Bucket {s3_bucket_name} does not exist in connected AWS account") 

    # Construct cluster creation
    cluster_config = Dict(
        cluster_name = name,
        instance_type = instance_type,
        max_num_workers = max_num_workers,
        initial_num_workers = initial_num_workers,
        min_num_workers = min_num_workers,
        aws_region = region,
        s3_read_write_resource = s3_bucket_arn,
        scaledown_time = scaledown_time,
        recreate = False,
    )

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
    send_request_get_response("create_cluster", cluster_config)

    if not nowait:
        wait_for_cluster(name)

    # Cache info
    get_cluster(name)

    return clusters[name]

    
def destroy_cluster(name: str, **kwargs):
    configure(**kwargs)
    logging.info(f"Destroying cluster named {name}")
    send_request_get_response("destroy_cluster", Dict[str, Any](cluster_name = name))


def delete_cluster(name: str, **kwargs):
    configure(**kwargs)
    logging.info(f"Deleting cluster named {name}")
    send_request_get_response("destroy_cluster", Dict[str, Any](cluster_name = name, permanently_delete = True))


def update_cluster(name: str, **kwargs):
    configure(**kwargs)
    logging.info(f"Updating cluster named {name}")
    send_request_get_response("update_cluster", Dict[str, Any](cluster_name = name))


def assert_cluster_is_ready(name: str, **kwargs):
    logging.info(f"Setting status of cluster named {name} to running") 
    configure(**kwargs)
    send_request_get_response("set_cluster_ready", Dict[str, Any](cluster_name = name))


class Cluster:
    def __init__(self, name: str, status: str, status_explanation: str, s3_bucket_arn: str):
        self.name = name
        self.status = status
        self.status_explanation = status_explanation
        self.s3_bucket_arn = s3_bucket_arn

def parsestatus(status: str):
    accepted = ["creating", "destroying", "updating", "failed", "starting",
    "stopped", "running", "terminated", "unknown"]
    if status in accepted:
        return status
    else:
        raise ValueError(f"Unexpected status {status}")


def get_clusters(cluster_name=None, **kwargs):
    logging.debug("Downloading description of clusters")
    configure(**kwargs)
    filters = dict()
    if cluster_name is not None:
        filters["cluster_name"] = cluster_name
    response = send_request_get_response("describe_clusters", {"filters":filters})
    clusters_dict = {
        name : Cluster(
            name,
            parsestatus(c["status"]),
            c["status_explanation"] if "status_explanation" in c else "",
            c["s3_read_write_resource"],
        ) for (name, c) in response["clusters"]
    }
    

    # Cache info
    global clusters
    for (name, c) in clusters_dict:
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
    if cluster_name is None:
        cluster_name = get_cluster_name()
    configure(**kwargs)
    return s3_bucket_arn_to_name(get_cluster_s3_bucket_arn(cluster_name))


def get_cluster(name: Optional[str]=None, **kwargs):
    if name is None:
        name = get_cluster_name()
    return get_clusters(name, **kwargs)[name]
    # global clusters
    # if name in clusters:
    #     if clusters[name].status is "failed":
    #         logging.error(c.status_explanation)
    # c = get_clusters(name, **kwargs)
    # if c.status is "failed":
    #     logging.error(c.status_explanation)
    # return c.status

def get_running_clusters(*args, **kwargs):
    return [entry for entry in get_clusters(*args, **kwargs) if entry[2].status == "running"]

def get_cluster_status(name:str=None, **kwargs):
    if name is None:
        name = get_cluster_name()
    global clusters
    if name in clusters:
        if clusters[name] == "failed":
            logging.error(c.status_explanation)
            # TODO: should c be clusters[name]

    c = get_cluster(name, **kwargs)[name]
    if c.status == "failed":
        logging.error(c.status_explanation)
    return c.status


def wait_for_cluster(name:str=None, **kwargs):
    # TODO: may need to be updated
    if name is None:
        name = get_cluster_name()
    t = 5
    p = progressbar.ProgressBar(max_value=progressbar.UnknownLength, widgets = [progressbar.DynamicMessage('status')])
    cluster_status = get_cluster_status(name, **kwargs)
    i = 0
    while cluster_status == "creating" or cluster_status == "updating":
        # if p is None:
        if cluster_status == "creating":
            p.dynamic_message['status'] = f"Setting up cluster {name}"
        else:
            p.dynamic_message['status'] = f"Updating cluster {name}"
        time.sleep(t)
        p.update(i)
        if t < 80:
            t *= 2
        cluster_status = get_cluster_status(name, **kwargs)
        i+=1
    if cluster_status == "running":
        logging.info(f"Cluster {name} is ready")
    elif cluster_status == "terminated":
        logging.error(f"Cluster {name} no longer exists")
    elif cluster_status not in ["creating", "updating"]:
        logging.error(f"Failed to set up cluster named {name}")
    else:
        logging.error(f"Cluster {name} has unexpected status: {cluster_status}")

def upload_to_s3(src_path, dst_name=None, cluster_name=None, **kwargs):
    if dst_name is None:
        dst_name = os.path.basename(src_path)
    if cluster_name is None:
        cluster_name = get_cluster_name()
    configure(**kwargs)
    bucket_name = get_cluster_s3_bucket_name(cluster_name)

    configure(**kwargs)

    if src_path.startswith("http://") or src_path.startswith("https://"):
        with urllib.request.urlopen(src_path) as f:
            s3.upload_fileobj(f, bucket_name, dst_name)
    if src_path.startswith("s://"):
        bucket_src, key_src = src_path.replace("s3://", "").split("/", 1)
        copy_source = {
            'Bucket': bucket_src,
            'Key': key_src
        }
        s3.meta_client.copy(copy_source, bucket_name, dst_name)
    else:
        if src_path.startswith("file://"):
            src_path = src_path[8:]
        src_path = Path(src_path)
        
        if src_path.is_file():
            s3.meta.client.upload_file(src_path, bucket_name, dst_name)
        else:
            files = [f for f in os.listdir(src_path) if os.path.isfile(os.path.join(src_path, f))]
            for file in files:
                s3.meta.client.upload_file(Path(os.path.join(src_path, file)), bucket_name, os.path.join(dst_name, file))
    return dst_name

def s3_bucket_arn_to_name(s3_bucket_arn: str):
    # Get s3 bucket name from arn
    s3_bucket_name = s3_bucket_arn.split(':')[-1]
    if s3_bucket_name.endswith("/") or s3_bucket_name.endswith("*"):
        # TODO: Is :-2 right or is it 1:-2
        s3_bucket_name = s3_bucket_name[:-2]
    elif s3_bucket_name.endswith("/*"):
        s3_bucket_name = s3_bucket_name[:-3]
    return s3_bucket_name