import logging
import os
from pathlib import Path
from _typeshed import NoneType
from typing import Any, Dict, Optional, Union

from botocore.config import Config
import boto3
import requests

from config import configure
from utils import send_request_get_response, get_aws_config_region

__all__ = [
    'logging',
    'os',
    'Path',
    'NoneType'
]