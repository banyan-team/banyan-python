from _typeshed import NoneType
import boto3
import warnings


def create_cluster(
    name:Union[str,NoneType]=None,
    instance_type:Union[str,NoneType]="m4.4xlarge",
    max_num_nodes:Union[str,NoneType]=8,
    iam_policy_arn:Union[str,NoneType]=None,
    s3_bucket_arn:Union[str,NoneType]=None,
    s3_bucket_name:Union[str,NoneType]=None,
    vpc_id=None,
    subnet_id=None,
    **kwargs,
):
    clusters = get_clusters(**kwargs) # TODO: where does this function come from?
    if name is None:
        name = "Cluster " + str(len(clusters) + 1)

    # Check if the configuration for this cluster name already exists
    # If it does, then recreate cluster
    if name in clusters:
        if clusters[name][status] == "terminated": # TODO: where does status come from?
            warnings.warn(f"Cluster configuration with {name} already exists. Ignoring new configuration and re-creating cluster.") # TODO: confirm this is what we want
            send_request_get_response("create_cluster", {"cluster_name": name, "recreate":True})
            return
        else:
            raise RuntimeError(f"Cluster with {name} already exists") # TODO: what kind of error should be raised?
        
    # Construct arguments

    # configure using parameters
    c = configure(require_ec2_key_pair_name=true, **kwargs) # TODO: where doe sthis function come from?

    if s3_bucket_arn is None and s3_bucket_name is None:
        s3_bucket_arn = "arn:aws:s3:::banyan-cluster-data-" + name + "-" + [random.randint(0,255) for _ in range(4)].hex() # TODO: is this right?
        s3_bucket_name = s3_bucket_arn.split(":")[-1]
        s3_create_bucket(get_aws_config(), s3_bucket_name) # TODO: where does the config come from?
    elif s3_bucket_arn is None:
        s3_bucket_arn = f"arn:aws:s3:::{s3_bucket_name}"
    elif s3_bucket_name is None:
        s3_bucket_name = s3_bucket_arn.split(":")[-1]

    if (s3_bucket_name not in s3_list_buckets(get_aws_config())) # TODO: ensure all functions defined
        raise RuntimeError(f"Bucket in {s3_bucket_name} does not exist in connected AWS account")
    
    # Construct cluster creation
    cluster_config = {
        "cluster_name": name,
        "instance_type": instance_type,
        "num_nodes": max_num_nodes,
        "ec2_key_pair": c["aws"]["ec2_key_pair_name"],
        "aws_region": get_aws_config_region(), # TODO: what is that?
        "s3_read_write_resource": s3_bucket_arn,
        "recreate": False
    }

    if iam_policy_arn is not None:
        cluster_config["additional_policy"] = iam_policy_arn
    
    if vpc_id is not None:
        cluster_config ["vpc_id"] = vpc_id

    if subnet_id is not None:
        cluster_config["subnet_id"] = subnet_id

    # Send request to create cluster
    send_request_get_response("create_cluster", cluster_config) # TODO: get this fn

    return Cluster(
        name,
        "creating",
        0,
        s3_bucket_arn
    )