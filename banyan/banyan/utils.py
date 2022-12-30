import hashlib
import inspect
import json
import os
import platform
from datetime import datetime
from typing import Dict

import boto3
import pytz
import requests
import toml

from .config import load_config
from .constants import BANYAN_API_ENDPOINT

s3_client = boto3.client("s3")

byte_sizes = {
    "kB": 10 ** 3,
    "MB": 10 ** 6,
    "GB": 10 ** 9,
    "TB": 10 ** 12,
    "PB": 10 ** 15,
    "KiB": 2 ** 10,
    "MiB": 2 ** 20,
    "GiB": 2 ** 30,
    "TiB": 2 ** 40,
    "PiB": 2 ** 50,
    "B": 1,
    "": 1,
}
byte_sizes = {k.lower(): v for (k, v) in byte_sizes.items()}


def get_aws_config_region():
    return s3_client.meta.region_name


def method_to_endpoint(method):
    return method.replace("_", "-")


def parse_bytes(s: str):
    s = s.replace(" ", "")
    if not any([char.isdigit() for char in s]):
        s = "1" + s

    index = -1
    for i in range(len(s)):
        if not s[i].isalpha():
            index = i + 1
            break

    prefix = s[:index]
    suffix = s[index:]

    n = -1
    try:
        n = float(prefix)
    except:
        raise ValueError(f"Could not interpret {prefix} as a number")

    multiplier = -1
    try:
        multiplier = byte_sizes[suffix.lower()]
    except:
        raise ValueError(f"Could not interpret {suffix} as a byte unit")

    result = n * multiplier
    return result


def send_request_get_response(method: str, content: dict):
    configuration = load_config()
    user_id = configuration["banyan"]["user_id"]
    api_key = configuration["banyan"]["api_key"]

    url = (BANYAN_API_ENDPOINT) + method_to_endpoint(method)
    content["debug"] = is_debug_on()
    headers = {
        "content-type": "application/json",
        "Username-APIKey": f"{user_id}-{api_key}",
    }
    resp = requests.post(
        url=url, json=content, headers=headers
    )  # , timeout=30)
    data = json.loads(resp.text)
    if resp.status_code == 403:
        raise Exception(
            "Please use a valid user ID and API key. Sign into the dashboard to retrieve these credentials."
        )
    elif resp.status_code == 504:
        # HTTP request timed out, for example
        if isinstance(data, Dict) and "message" in data:
            data = data["message"]
        return None
    elif resp.status_code == 500 or resp.status_code == 504:
        raise Exception(data)
    elif resp.status_code == 502:
        raise Exception(
            "Sorry there has been an error. Please contact support."
        )
    return data


def is_debug_on():
    return os.environ.get("PYTHON_DEBUG", "") == "Banyan"
    # return logging.DEBUG >= logging.root.level


def get_python_version():
    """Gets the python version.

    Returns
    -------
    str
        The Python version returned as a string
    """
    return platform.python_version()


def parse_time(time):
    """Converts given time to local timezone.

    Parameters
    ---------
    time : str
        The current time in the format "yyyy-mm-dd-HH:MM:SSzzzz"

    Returns
    -------
    str
        The `DateTime` for the given time string
    """
    time = datetime.fromisoformat(time)
    timezone = pytz.timezone("UTC")
    time = timezone.localize(time)
    local_time = time.astimezone()
    return local_time


def get_loaded_packages():
    """Returns all the packages/libraries that are currently imported by the user"""
    return [
        p[0]
        for p in locals().items()
        if inspect.ismodule(p[1]) and not p[0].startswith("__")
    ]


def get_hash(s):
    """Gets a unique representation of a string

    Parameters
    ----------
    s : str
        The string to take a hash of

    Returns
    -------
    str
        The SHA256 hash of the string

    """
    hs = hashlib.sha256(s.encode("utf-8")).hexdigest()
    return hs


def load_toml(path):
    if isinstance(path, list):
        result = {}
        for p in path:
            result.update(load_toml(p))
        return result

    if path.startswith("file://"):
        return toml.load(path[7:-1])

    elif path.startswith("s3://"):
        raise Exception("S3 path not currently supported")

    elif (path.startswith("http://")) or (path.startswith("https://")):
        (requests.get(path)).content
        return toml.loads(requests.get(path).text)
