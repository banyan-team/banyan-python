import os

from sessions import get_sessions

def test_get_sessions():
    """Will test getting sessions
    """

    cluster_name = os.environ['BANYAN_CLUSTER_NAME']
    get_sessions(cluster_name)

