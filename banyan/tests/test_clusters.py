import os
from banyan.clusters import update_cluster
from banyan.clusters import wait_for_cluster
import pytest
from banyan.clusters import (
    create_cluster,
    get_clusters,
    get_running_clusters,
    destroy_cluster,
    get_cluster_status,
    delete_cluster,
)


def test_create_run_destroy():
    # tests a bad name
    # print("testing a bad name (it should give an error)")
    # with pytest.raises(Exception) as excinfo:
    #     bad_cluster_name = 'stanley cluster'
    #     cluster_object = create_cluster(
    #         name = bad_cluster_name,
    #         instance_type = "t3.2xlarge",
    #         region = "us-west-2",
    #         ec2_key_pair_name = "CailinSSHKeyPair"
    #     )
    # assert str(excinfo.value) == 'Cluster name can only contain lowercase letters, numbers, and hyphens'

    # print("testing create a cluster")
    cluster_name = "stanley-session-woof2-cluster"
    update_cluster(cluster_name)
    # cluster_object = create_cluster(
    #     name = cluster_name,
    #     instance_type = "t3.2xlarge",
    #     region = "us-west-2",
    #     ec2_key_pair = "CailinSSHKeyPair"
    # )
    # # list_clusters = get_clusters()
    # # print("clusters:")
    # # print(list_clusters)

    # # assert cluster_name in get_clusters()
    # # print("hi!")
    # # print(get_cluster_status(cluster_name))
    # # wait_for_cluster(cluster_name)

    # # assert cluster_name in get_running_clusters()

    # # delete_cluster(cluster_name)

    # # assert cluster_name in get_clusters()

    # # assert not cluster_name in get_running_clusters()

    # #  cluster_name = os.environ['BANYAN_CLUSTER_NAME']
    # print("normal")
    # print(get_clusters())
    # print("running")
    # print(get_running_clusters())
