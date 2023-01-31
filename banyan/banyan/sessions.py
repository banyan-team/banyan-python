import io
import json
import os
import shutil
import time
import zipfile

import boto3
from botocore.exceptions import ClientError

from banyan.config import configure
from banyan.utils import convert_iso_time, send_request_get_response


SessionId = str
SUPPORTED_PYTHON_VERSIONS = ["3.7", "3.8", "3.9"]

_iam_client = boto3.client("iam")
_lambda_client = boto3.client("lambda")
_s3_client = boto3.client("s3")


def _get_executor_code_from_s3():
    return _s3_client.get_object(
        Bucket="banyan-executor",
        Key="executor.py"
    )


def _get_executor_lambda_zip(site_packages_dir: str):
    # Construct zipfile with site_packages_dir if provided
    zipfile_name = "executor_lambda_code"
    if site_packages_dir:
        shutil.make_archive(zipfile_name, "zip", root_dir=site_packages_dir, base_dir=".")
    else:
        # Create empty zipfile
        with zipfile.ZipFile(zipfile_name + ".zip", "w") as f:
            pass
    # Add executor code file
    executor_code = _get_executor_code_from_s3()["Body"].read().decode("utf-8")
    with zipfile.ZipFile(zipfile_name + ".zip", "a") as f:
        f.writestr("lambda_function.py", executor_code)
    # Upload to S3. First create a S3 bucket if not already existing
    # and then upload the zip file.
    bucket_name = "banyan-assets"
    s3_key = zipfile_name + ".zip"
    try:
        _s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": _s3_client.meta.region_name}
        )
        _s3_client.get_waiter("bucket_exists").wait(Bucket=bucket_name)
    except ClientError as error:
        if not error.response["Error"]["Code"] in ["BucketAlreadyExists", "BucketAlreadyOwnedByYou"]:
            raise
    with open(zipfile_name + ".zip", "rb") as f:
        _s3_client.put_object(
            Bucket=bucket_name,
            Body=f,
            Key=s3_key
        )
    return bucket_name, s3_key


def _update_executor_code(
    executor_lambda_function_name: str,
    lambda_last_modified: int
):
    executor_code_object =_get_executor_code_from_s3()
    executor_code_last_modified = executor_code_object["LastModified"].timestamp()
    if executor_code_last_modified > lambda_last_modified:
        # Update executor code
        bucket_name, s3_key = _get_executor_lambda_zip()
        _s3_client.update_function_code(
            FunctionName=executor_lambda_function_name,
            S3Bucket=bucket_name,
            S3Key=s3_key,
        )


def _create_executor_lambda_iam_role():
    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            },
        ]
    }
    basic_lambda_policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
    iam_role_name = "banyan-executor-lambda-role"
    try:
        role = _iam_client.create_role(
            RoleName=iam_role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )
        _iam_client.get_waiter("role_exists").wait(RoleName=iam_role_name)
        _iam_client.attach_role_policy(RoleName=iam_role_name, PolicyArn=basic_lambda_policy_arn)
        time.sleep(5)
    except ClientError as error:
        if error.response["Error"]["Code"] == "EntityAlreadyExists":
            role = _iam_client.get_role(RoleName=iam_role_name)
        else:
            raise
    return role["Role"]["Arn"]


def _create_executor_lambda_function(
    site_packages_dir: str, version: str, environment_hash: str
):
    """Creates an executor Lambda if one doesn't exist with the same hash."""
    executor_lambda_function_name = (
        f"executor_python{version.replace('.', '-')}_{environment_hash}"
    )
    # Note that there may be race conditions here, since creating a lambda might
    # take some time.
    # Check if the function exists, and if it does not, create one
    try:
        executor_info = _lambda_client.get_function(
            FunctionName=executor_lambda_function_name
        )
        last_modified = int(
            convert_iso_time(executor_info["Configuration"]["LastModified"])
        )
        _update_executor_code(
            executor_lambda_function_name=executor_lambda_function_name,
            lambda_last_modified=last_modified,
        )
    except _lambda_client.exceptions.ResourceNotFoundException:    
        # Zip together directory and create function
        bucket_name, s3_key = _get_executor_lambda_zip(site_packages_dir)
        _lambda_client.create_function(
            FunctionName=executor_lambda_function_name,
            Runtime=f"python{version}",
            Role=_create_executor_lambda_iam_role(),
            Handler="executor.lambda_handler",
            Code={
                "S3Bucket": bucket_name,
                "S3Key": s3_key,
            },
            Timeout=900,
            MemorySize=10240,
        )


def _compute_environment_hash(site_packages_dir: str):
    """Computes last modified date of the given directory/subdirectories."""
    if not site_packages_dir:
        return "0000"
    last_modified = int(
        max(os.path.getmtime(root) for root,_,_ in os.walk(site_packages_dir))
    )
    return last_modified


def start_session(
    num_workers: int = 16,
    python_version: str = "3.8",
    session_name: str = None,
    site_packages_dir: str = None,
    *args,
    **kwargs,
):
    """Starts a new session."""
    configure(*args, **kwargs)

    if python_version not in SUPPORTED_PYTHON_VERSIONS:
        raise ValueError(
            f"Only the following Python versions are supported: "
            f"{SUPPORTED_PYTHON_VERSIONS}"
        )
    environment_hash = _compute_environment_hash(site_packages_dir=site_packages_dir)
    _create_executor_lambda_function(
        site_packages_dir=site_packages_dir,
        version=python_version,
        environment_hash=environment_hash,
    )
    response = send_request_get_response(
        "start-session",
        {
            "num_workers": num_workers,
            "version": python_version,
            "environment_hash": environment_hash,
            "aws_region": _lambda_client.meta.region_name,
            "session_name": session_name,
        },
    )


def get_session_id() -> SessionId:
    return ""
