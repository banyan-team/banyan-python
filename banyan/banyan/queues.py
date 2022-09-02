import boto3
from datetime import datetime
import json
from typing import Any, Dict, Tuple

from id import generate_message_id, ResourceId
from sessions import get_session, get_session_id, end_session
from utils import from_py_value_contents, to_py_value_contents
from utils_pfs import from_py_value_contents

sqs = boto3.client("sqs")

#################
# GET QUEUE URL #
#################


def get_scatter_queue(resource_id=None):
    if resource_id is None:
        resource_id = get_session().resource_id
    return sqs.get_queue_url(QueueName="banyan_" + resource_id + "_scatter.fifo")[
        "QueueUrl"
    ]


def get_gather_queue(resource_id=None):
    if resource_id is None:
        resource_id = get_session().resource_id
    return sqs.get_queue_url(QueueName="banyan_" + resource_id + "_gather.fifo")[
        "QueueUrl"
    ]


def get_execution_queue(resource_id=None):
    if resource_id is None:
        resource_id = get_session().resource_id
    return sqs.get_queue_url(QueueName="banyan_" + resource_id + "_execution.fifo")[
        "QueueUrl"
    ]


###################
# RECEIVE MESSAGE #
###################


def get_next_message(
    queue,
    delete=True,
    error_for_main_stuck: str = None,
    error_for_main_stuck_time: datetime.DateTime = None,
):
    error_for_main_stuck = check_worker_stuck(
        error_for_main_stuck, error_for_main_stuck_time
    )
    message_receiving_result = sqs.receive_message(
        QueueUrl=queue, MaxNumberOfMessages=1
    )
    m = (
        message_receiving_result["Messages"][0]["ReceiptHandle"]
        if len(message_receiving_result["Messages"]) > 0
        else None
    )

    while m is None:
        message_receiving_result = sqs.receive_message(
            QueueUrl=queue, MaxNumberOfMessages=1
        )
        m = (
            message_receiving_result["Messages"][0]["ReceiptHandle"]
            if len(message_receiving_result["Messages"]) > 0
            else None
        )

    if delete:
        sqs.delete_message(QueueUrl=queue, ReceiptHandle=m)
    return message_receiving_result["Messages"][0]["Body"], error_for_main_stuck


def receive_next_message(
    queue_name,
    error_for_main_stuck: str = None,
    error_for_main_stuck_time: datetime.DateTime = None,
):
    content, error_for_main_stuck = get_next_message(
        queue_name,
        error_for_main_stuck=error_for_main_stuck,
        error_for_main_stuck_time=error_for_main_stuck_time,
    )
    end_str = "MESSAGE_END"
    if content.startswith("JOB_READY") or content.startswith("SESSION_READY"):
        response = {"kind": "SESSION_READY"}
    elif content.startswith("EVALUATION_END"):
        print(content[14 : -(len(end_str) if content.endswith(end_str) else 0)])
        response = {"kind": "EVALUATION_END", "end": content.endswith(end_str)}
    elif content.startswith("JOB_FAILURE") or content.startswith("SESSION_FAILURE"):
        tail = len(end_str) if content.endswith(end_str) else 0
        head_len = (
            len("JOB_FAILURE")
            if content.startswith("JOB_FAILURE")
            else len("SESSION_FAILURE")
        )
        # This print statement is needed, so that we can print out the error message
        print(content[head_len:-tail])
        if content.endswith(end_str):
            end_session(
                failed=True, release_resources_now=content.startswith("JOB_FAILURE")
            )
            raise RuntimeError("Session failed; see preceding output")
        response = {"kind": "SESSION_FAILURE"}
    else:
        response = json.loads(content)
    return response, error_for_main_stuck


# Used by Banyan/src/pfs.jl, intended to be called from the executor
def receive_from_client(value_id):
    # Send scatter message to client
    send_message(
        get_gather_queue(),
        json.dumps({"kind": "SCATTER_REQUEST", "value_id": value_id}),
    )
    # Receive response from client
    m = json.loads(get_next_message(get_scatter_queue()))
    v = from_py_value_contents(m["contents"])
    return v


################
# SEND MESSAGE #
################


def send_message(queue_name, message):
    # queue_url = sqs.get_queue_url(queue_name)
    return sqs.send_message(
        QueueUrl=queue_name,  # queue_url,
        MessageBody=message,  # TODO: Is that correct?,
        MessageGroupId="1",
        MessageDeduplicationId=generate_message_id(),
    )


def send_to_client(value_id, value, worker_memory_used=0):
    MAX_MESSAGE_LENGTH = 220000
    message = to_py_value_contents(value)
    i = 0
    while True:
        is_last_message = len(message) <= MAX_MESSAGE_LENGTH
        if not is_last_message:
            msg = message[:MAX_MESSAGE_LENGTH]
            message = message[MAX_MESSAGE_LENGTH:]
        msg = {
            "kind": "GATHER_END" if is_last_message else "GATHER",
            "value_id": value_id,
            "contents": message,
            "worker_memory_used": worker_memory_used,
            "gather_page_idx": i,
        }
        send_message(get_gather_queue(), json.dumps(msg))
        i += 1
        if is_last_message:
            break
