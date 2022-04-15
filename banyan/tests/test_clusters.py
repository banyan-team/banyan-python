import os
import random
import string


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



def test_create_cluster_with_invalid_name():
    with pytest.raises(Exception) as excinfo:
        bad_cluster_name = "name with spaces"
        cluster_object = create_cluster(
            name = bad_cluster_name,
            instance_type = "t3.xlarge",
        )
    assert "can only contain" in str(excinfo.value)


def test_create_delete_cluster():

    cluster_name = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))

    cluster_object = create_cluster(
        name = cluster_name,
        instance_type = "t3.xlarge",
    )

    assert cluster_name in get_clusters()
 
    assert cluster_name in get_running_clusters()

    delete_cluster(cluster_name)

    assert cluster_name in get_clusters()

    assert not cluster_name in get_running_clusters()

