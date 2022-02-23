from .imports import *
from operator import truediv
import boto3
import hashlib
import inspect
import logging
import os
import platform
import pytz 
import requests
import toml
import urllib3

from botocore.exceptions import ClientError
from .config import load_config
from datetime import datetime
from typing import Dict

s3_client = boto3.client('s3')

s3 = boto3.client('s3')

def get_aws_config_region():
    return s3.meta.region_name
    # c = Config()
    # return c.region_name


def send_request_get_response(endpoint: str, content:dict):
    configuration = load_config()
    user_id = configuration["banyan"]["user_id"]
    api_key = configuration["banyan"]["api_key"]
    if os.get_env("BANYAN_API_ENDPOINT", default=None) is None:
        banyan_api_endpoint = "https://4whje7txc2.execute-api.us-west-2.amazonaws.com/prod/"
    else:
        banyan_api_endpoint = os.get_env("BANYAN_API_ENDPOINT", default=None)
    url = str(banyan_api_endpoint, endpoint)
    # content["debug"] = is_debug_on()
    headers = {
        "content-type": "application/json",
        "Username-APIKey": f"{user_id}-{api_key}"
    }
    resp = requests.post(url=url, data=content, headers=headers)
    data = resp.text

    if resp.status_code == 403:
        raise Exception("Please use a valid user ID and API key. Sign into the dashboard to retrieve these credentials.")
    elif resp.status_code == 504:
        # HTTP request timed out, for example
        if isinstance(data, Dict) and "message" in data:
            data = data["message"]
        # @error data #?????
        return None
    elif resp.status_code == 500 or resp.status_code == 504:
        raise Exception(data)
    elif resp.status == 502:
        raise Exception("Sorry there has been an error. Please contact support.")
    return data

def is_debug_on():
    return logging.DEBUG >= logging.root.level

def get_python_version():
    """Gets the python version.
    
    Returns
    -------
    string
        Python version is returned as a string
    """
    return (platform.python_version())

def parse_time(time):
    """Converts given time to local timezone.
    
    Parameters
    ---------
    time : string
        The current time in the format "yyyy-mm-dd-HH:MM:SSzzzz"
    Returns
    -------
    string
        The DateTime is returned.
    """
    time = datetime.strptime(time[:-4], '%Y-%m-%d-%H:%M:%S') #we don't want milli-second
    timezone = pytz.timezone("UTC")
    time = timezone.localize(time)
    local_time = time.astimezone()
    return local_time    

#TO DO - to test this function
def get_loaded_packages():
    """Returns all the packages/libraries that are currently imported by the user
    """
    return [
        p[0]
        for p in locals().items()
        if inspect.ismodule(p[1]) and not p[0].startswith('__')
    ]
    
def get_hash(s):
    """Gets a unique represetation of a string
    
    Parameters
    ----------
    s : string

    Returns
    -------
    string :
        the SHA256 hash of the string
    
    """
    hs = hashlib.sha256(s.encode('utf-8')).hexdigest()

def upload_file_to_s3(filename, bucket, object_name = None):
    """Uploads file to the S3 bucket

    Parameters
    ----------
    filename: string
        Is the local path to the file to upload
    bucket: string
        Is the S3 bucket to which to upload the file to

    Returns
    -------
    boolean: True if file was uploaded, else False
    """

    #if S3 object_name not specified, use filename
    key = os.path.basename(filename)

    # upload the file
    try:
        reponse = s3_client.upload_file(filename, bucket, object_name if object_name is not None else key)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def load_toml(path):
    # path --> "file://.banyan/banyanconfig.toml"
    # path[2:], path[4:8], path[:9]
    if path.startswith('file://'):
        toml.loads(path[8:])
    
    elif path.startswith('s3://'):
        raise Exception("S3 path not currently supported")

    elif (path.startswith('http://')) or (path.startswith('https://')):
        r = (urllib3.PooManager().request('GET', path)).data #downloads the data from the internet into a toml-fomatted string
        data = toml.loads(r.decode("utf-8"))#loads the toml-formatted string 
