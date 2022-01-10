from typing import Dict
import requests
from config import load_config


def method_to_string(method) = begin
    if str(method).replace("_", "-") in ["create-cluster","destroy-cluster","describe-clusters",
    "create-job","destroy-job","describe-jobs","evaluate","update-cluster","set-cluster-ready"):
        return str(method)


def send_request_get_response(method: str, content:dict):
    # TODO: load configuration
    configuration = load_config()
    user_id = configuration["banyan"]["user_id"]
    api_key = configuration["banyan"]["api_key"]
    url = str(BANYAN_API_ENDPOINT, method_to_string(method))
    content["debug"] = is_debug_on()
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
    elif resp.status == 502
        raise Exception("Sorry there has been an error. Please contact support.")
    return data