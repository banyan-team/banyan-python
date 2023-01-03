from utils import send_request_get_response


SessionId = str
SUPPORTED_PYTHON_VERSIONS = ["3.7", "3.8", "3.9"]

iam_client = boto3.client("iam")
lambda_client = boto3.client("lambda")


def _get_executor_lambda_zip(site_packages_dir: str):
    # TODO: Download the executor function
    # TODO: Copy the site packages dir into the zip file
    # TODO: Any cleanup if needed
    # TODO: Read the bytes and return that (if too big, will need to upload to s3 instead)
    pass


def _create_executor_lambda_function(
    site_packages_dir: str, version: str, environment_hash: str
):
    executor_lambda_function_name = (
        f"executor_python{version.replace('.', '-')}_{environment_hash}"
    )
    # Note that there may be race conditions here, since creating a lambda might
    # take some time.
    # TODO: Check if the function exists.
    # TODO: If it does not, create one.
    if not lambda_client.check_lambda_exists(executor_lambda_function_name):
        raise BanyanError(
            f"{executor_lambda_function_name} does not exist. Please contact "
            "support if this error persists."
        )
    # TODO: Create role
    # Zip together directory and create function
    lambda_client.create_function(
        FunctionName=executor_lambda_function_name,
        Runtime=f"python{version}",
        # Role,
        Handler="executor.lambda_handler",
        Code={
            "ZipFile": _get_executor_lambda_zip(site_packages_dir),
        },
        Timeout=900,
        MemorySize=10240,
    )


def _compute_environment_hash(site_packages_dir: str):
    # TODO: Compute this
    return ""


def start_session(
    num_workers: int = 16,
    python_version: str = "3.8",
    session_name: str = None,
    site_packages_dir: str = None,
):
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
    aws_region = lambda_client.meta.region_name
    response = send_request_get_response(
        "start-session",
        {
            "num_workers": num_workers,
            "version": python_version,
            "environment_hash": environment_hash,
            "aws_region": lambda_client.meta.region_name,
            "session_name": session_name,
        },
    )


def get_session_id() -> SessionId:
    return ""
import boto3
