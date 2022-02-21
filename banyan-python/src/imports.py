# Standard Library
import logging
import os
from pathlib import Path
import time
from typing import Any, Dict, Optional, Union
# from _typeshed import NoneType

# Third Party
import boto3
from botocore.config import Config
import progressbar
import requests
import toml

# Constructed
NoneType = type(None)