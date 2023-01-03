import logging
import os

import boto3

__version__ = "0.2.0"

# Check if AWS region is set. If not, default to us-west-2 and give a warning
if boto3.Session().region_name == None:
    logging.warning(
        "Defaulting to region us-west-2. If you want to use a different AWS region, "
        "please set the `AWS_DEFAULT_REGION` environment variable or update the "
        "default region in `~/.aws/config`, before importing `banyan`."
    )
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"

from .annotation import Future, record_task
from .config import configure
from .constants import *
from .utils_future_computation import PartitionType
