import os
from src.clusters import wait_for_cluster

from src.clusters import create_cluster, get_clusters, get_running_clusters, destroy_cluster, get_cluster_status

def test_create_run_destroy():
    cluster_name = 'stanley-cluster4'
    cluster_object = create_cluster(
        name = cluster_name,
        instance_type = "t3.2xlarge",
        region = "us-west-2", 
        ec2_key_pair_name = "CailinSSHKeyPair" 
    )
    # list_clusters = get_clusters()
    # print("clusters:")
    # print(list_clusters)

    # assert cluster_name in get_clusters() 
    print("hi!")
    print(get_cluster_status(cluster_name))
    # wait_for_cluster(cluster_name)

    # assert cluster_name in get_running_clusters()

    # destroy_cluster(cluster_name)

    # assert cluster_name in get_clusters()

    # assert not cluster_name in get_running_clusters()

def test_get_clusters():
    #  cluster_name = os.environ['BANYAN_CLUSTER_NAME']
    print(get_clusters())   

