__version__ = "0.1.1"

import boto3
import logging

# Check if AWS region is set. If not, default to us-west-2 and give a warning
if boto3.Session().region_name == None:
    logging.warning(
        "Defaulting to region us-west-2. If you want to use a different AWS region, "
        "please set the `AWS_DEFAULT_REGION` environment variable or update the "
        "default region in `~/.aws/config`, before importing `banyan`."
    )
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"

from .constants import *
from .clusters import (
    create_cluster,
    destroy_cluster,
    delete_cluster,
    update_cluster,
    assert_cluster_is_ready,
    Cluster,
    get_clusters,
    get_cluster_s3_bucket_arn,
    get_cluster_s3_bucket_name,
    get_cluster,
    get_running_clusters,
    get_cluster_status,
    wait_for_cluster,
    upload_to_s3,
)
from .config import load_config, write_config, configure
from .id import generate_message_id
from .locations import Client
from .queues import (
    get_scatter_queue,
    get_gather_queue,
    get_execution_queue,
    get_next_message,
    receive_next_message,
    receive_from_client,
    send_message,
    send_to_client,
)
from .session import Session, set_session, get_session_id, get_session, get_cluster_name
from .sessions import (
    end_session,
    end_all_sessions,
    get_sessions,
    get_running_sessions,
    get_session_status,
    download_session_logs,
    wait_for_session,
    start_session,
    run_session,
)
from .utils import (
    get_aws_config_region,
    send_request_get_response,
    is_debug_on,
    get_python_version,
    parse_time,
    get_loaded_packages,
    get_hash,
    upload_file_to_s3,
    load_toml,
)
