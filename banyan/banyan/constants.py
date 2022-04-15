import os

BANYAN_PYTHON_BRANCH_NAME = "claris+melany/banyan-python"
BANYAN_PYTHON_PACKAGES = ["banyan"]
BANYAN_API_ENDPOINT = os.getenv(
    "BANYAN_API_ENDPOINT",
    default="https://4whje7txc2.execute-api.us-west-2.amazonaws.com/prod/",
)
