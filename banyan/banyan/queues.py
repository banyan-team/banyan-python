import boto3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from typing import Any, Dict, Tuple

from .id import generate_message_id, ResourceId
from .sessions import get_session, get_session_id, end_session
from .utils import from_py_string, to_py_string

sqs = boto3.client("sqs")

#################
# GET QUEUE URL #
#################


def scatter_queue_url():
    return get_session().scatter_queue_url


def gather_queue_url():
    return get_session().gather_queue_url


def execution_queue_url():
    return get_session().execution_queue_url


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
        gather_queue_url(),
        json.dumps({"kind": "SCATTER_REQUEST", "value_id": value_id}),
    )
    # Receive response from client
    m = json.loads(get_next_message(gather_queue_url()))
    v = from_py_string(m["contents"])
    return v


################
# SEND MESSAGE #
################


def send_message(queue_url, message, group_id="1", deduplication_id=None):
    if deduplication_id is None:
        deduplication_id = generate_message_id()
    return sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message,  # TODO: Is that correct?,
        MessageGroupId=group_id,
        MessageDeduplicationId=deduplication_id,
    )


def send_to_client(value_id, value, worker_memory_used=0):
    MAX_MESSAGE_LENGTH = 220000
    message = to_py_string(value)
    generated_message_id = generate_message_id()

    # Break the message down into chunk ranges
    nmessages = 0
    message_length = len(message)
    message_ranges = []
    message_i = 0
    while True:
        is_last_message = message_length <= MAX_MESSAGE_LENGTH
        starti = message_i
        if is_last_message:
            message_i += message_length
            message_length = 0
        else:
            message_i += MAX_MESSAGE_LENGTH
            message_length -= MAX_MESSAGE_LENGTH
        message_ranges.append((starti, message_i))
        nmessages += 1
        if is_last_message:
            break

    # Launch asynchronous threads to send SQS messages
    gather_q_url = gather_queue_url()
    num_chunks = len(message_ranges)
    if num_chunks > 1:
        with ThreadPoolExecutor() as executor:
            executor.map(
                lambda i: send_message(
                    gather_q_url,
                    json.dumps(
                        {
                            "kind": "GATHER",
                            "value_id": value_id,
                            "contents": message[
                                message_ranges[i][0] : message_ranges[i][1]
                            ],
                            "contents_length": message_ranges[i][1]
                            - message_ranges[i][0],
                            "worker_memory_used": worker_memory_used,
                            "chunk_idx": i,
                            "num_chunks": num_chunks,
                        }
                    ),
                    group_id=str(i),
                    deduplication_id=generated_message_id + str(i),
                ),
                list(range(num_chunks)),
            )
    else:
        i = 0
        msg = {
            "kind": "GATHER",
            "value_id": value_id,
            "contents": message[message_ranges[i][0] : message_ranges[i][1]],
            "worker_memory_used": worker_memory_used,
            "chunk_idx": i,
            "num_chunks": num_chunks,
        }
        msg_json = json.dumps(msg)
        send_message(
            gather_q_url,
            msg_json,
            group_id=str(i),
            deduplication_id=generated_message_id + str(i),
        )
