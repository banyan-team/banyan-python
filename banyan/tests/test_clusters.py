import os

from src.clusters import create_cluster, get_clusters, get_running_clusters, destroy_cluster

def test_create_run_destroy():
    cluster_id = create_cluster(
        name = "cluster-testing",
        instance_type = "t3.2xlarge",
        region = "us-west-2", 
        ec2_key_pair_name = "CailinSSHKeyPair" 
    )
   
    assert get_clusters().has_key(cluster_id) 

    assert get_running_clusters().has_key(cluster_id)

    destroy_cluster(cluster_id)

    assert get_clusters().has_key(cluster_id)

    assert not get_running_clusters().has_key(cluster_id)

def test_get_clusters():
    #  cluster_name = os.environ['BANYAN_CLUSTER_NAME']
    print(get_clusters())   

