import os

BANYAN_PYTHON_BRANCH_NAME = "v22.06.17"
BANYAN_PYTHON_PACKAGES = ["banyan"]
BANYAN_API_ENDPOINT = os.getenv(
    "BANYAN_API_ENDPOINT",
    default="https://4whje7txc2.execute-api.us-west-2.amazonaws.com/prod/",
)

NUM_PROCESSES_PER_INVOCATION = 6

MAX_SQS_MESSAGE_LENGTH = 220_000