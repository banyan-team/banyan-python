import json
import math
from typing import Any, List, Optional

from .utils_serialization import from_str, to_str
from .utils import batch

import boto3


worker_configuration = None

def is_main_invocation(invocation_idx) -> bool:
    return invocation_idx == 0

def is_main_process(process_or_worker_idx) -> bool:
    from .constants import NUM_PROCESSES_PER_INVOCATION
    return (process_or_worker_idx % NUM_PROCESSES_PER_INVOCATION) == 0

def configure_worker(
    num_workers,
    # Invocation-level configuration
    invocation_idx,
    scatter_queue_url,
    gather_queue_url,
    shuffle_queue_urls,
    # Process-level configuration
    process_idx,
    process_pipes
):
    """
    Configures Banyan worker with information about the current serverless
    function invocation and Python process within that invocation

    Arguments
    ---------
    invocation_idx : int
        The index of this serverless function invocation
    scatter_queue_url : str
        The URL for SQS queue for sending data from client side to the main
        invocation.
    gather_queue_url : str
        The URL for SQS queue for gathering data from the main invocation to
        the client side.
    shuffle_queue_urls : List[str]
        Contains the URL for an SQS queue for each serverless function
        invocation for sending/receiving messages to/from it. The length of
        this list should be the number of invocations - not the number of
        workers.
    process_idx : int
        The index of this Python process in the current serverless function
        invocation. For AWS Lambda, this is between 0 and 5.
    process_pipes : List[multiprocessing.Pipe]
        One pipe for sending/receiving to/from the main process if this is not
        the main process or if this is the main process, a pipe to send/receive
        to/from each non-main process.
    """
    global worker_configuration
    worker_configuration = {
        "num_workers": num_workers,
        "invocation_idx": invocation_idx,
        "sqs": boto3.client("sqs") if is_main_process(process_idx) else None,
        "scatter_queue_url": scatter_queue_url,
        "gather_queue_url": gather_queue_url,
        "shuffle_queue_urls": shuffle_queue_urls,
        "process_idx": process_idx,
        "process_pipes": process_pipes
    }


def get_worker_config():
    global worker_configuration
    if worker_configuration is None:
        raise Exception("No configuration for current worker")
    return worker_configuration


def get_num_workers():
    return get_worker_config()["num_workers"]


def get_num_invocations():
    from .constants import NUM_PROCESSES_PER_INVOCATION
    return int(math.ceil(get_worker_config()["num_workers"] / NUM_PROCESSES_PER_INVOCATION))


def get_invocation_idx(worker_idx=None):
    from .constants import NUM_PROCESSES_PER_INVOCATION
    if worker_idx is None:
        return get_worker_config()["invocation_idx"]
    else:
        return worker_idx // NUM_PROCESSES_PER_INVOCATION


def is_same_invocation(worker_idx_a, worker_idx_b) -> bool:
    return get_invocation_idx(worker_idx_a) == get_invocation_idx(worker_idx_b)


def get_process_idx(worker_idx=None):
    from .constants import NUM_PROCESSES_PER_INVOCATION
    return (
        get_worker_config()["process_idx"]
        if worker_idx is None
        else worker_idx % NUM_PROCESSES_PER_INVOCATION
    )

def get_num_processes():
    from .constants import NUM_PROCESSES_PER_INVOCATION
    if get_invocation_idx() == get_num_invocations() - 1:
        return get_num_workers() % NUM_PROCESSES_PER_INVOCATION
    return NUM_PROCESSES_PER_INVOCATION


def get_worker_idx():
    from .constants import NUM_PROCESSES_PER_INVOCATION
    return get_invocation_idx() * NUM_PROCESSES_PER_INVOCATION + get_process_idx()


def sqs():
    return get_worker_config()["sqs"]


def get_queue_url(dst=None, src=None):
    if src == "client":
        assert dst == 0 or dst == None
        return get_worker_config()["scatter_queue_url"]
    elif dst == "client":
        assert src == 0 or src == None
        return get_worker_config()["gather_queue_url"]
    else:
        dst = dst if dst is not None else 0
        dst = get_invocation_idx(dst)
        return get_worker_config()["shuffle_queue_urls"][dst]


def get_pipe(dst_worker_idx=None):
    process_pipes = get_worker_config()["process_pipes"]
    return process_pipes[
        0
        if (dst_worker_idx is None or len(process_pipes) == 1)
        else get_process_idx(dst_worker_idx)
    ]

def is_main_worker(worker_idx):
    return worker_idx == 0

"""
Number of SQS messages sent from this process
"""
num_messages_sent = 0

NO_DATA = "BANYAN_NO_DATA"

def shuffle(data: List[Any]) -> List[Any]:
    """
    Sends and receives a list of data with an element for each worker

    Given a list of data with an element for each worker, sends each element
    to its destination worker and then receives data from each worker and
    returns a list with an element for each worker received from.
    """
    if len(data) != get_num_workers():
        raise ValueError(
            "Expected an element in the list for each of the "
            f"{get_num_workers()} workers to shuffle the data across."
        )
    return send_receive("shuffle", data)

def gather(data: Any) -> List[Any]:
    """
    Gathers data in a list to the main worker with an element for each worker

    Arguments
    ---------
    data : Any
        Each worker supplies some data that is then collected into a list
        on the main worker.
    """
    return send_receive("gather", data)

def scatter(data: Optional[List[Any]] = NO_DATA) -> List[Any]:
    """
    Sends a list of data with an element for each worker to send to from the
    main worker.

    Arguments
    ---------
    data : Optional[List[Any]]
        Only specify if called from main worker
    """
    if (data != NO_DATA) == is_main_worker():
        raise ValueError(
            "Data should only be specified for the main worker it is to be "
            "scattered from."
        )
    return send_receive("scatter", data)

def send_receive(pattern, data):
    """
    Sends/receives the given data with the given pattern

    There are 3 kinds of communication patterns:
    - shuffle - send to all workers - receive from all workers
    - gather - send to main worker - receive from all workers
    - scatter - send to all workers - receive from one worker

    Arguments
    ---------
    pattern
    data
    """
    global num_messages_sent

    # Get current worker's info
    curr_worker_idx = get_worker_idx()
    SQS = sqs()

    # Make send_to
    send_to = []
    if pattern == "shuffle":
        send_to = range(get_num_workers())
    elif pattern == "gather":
        send_to = [0]
    elif pattern == "scatter" and is_main_worker(curr_worker_idx):
        send_to = range(get_num_workers())

    # Convert arguments to lists
    if data == NO_DATA:
        data = []
    elif not isinstance(data, list):
        data = [data]

    # Stringify and batch each piece of data
    from .constants import MAX_SQS_MESSAGE_LENGTH 
    data = [list(batch(to_str(d), MAX_SQS_MESSAGE_LENGTH)) for d in data]

    # Expand data to match length of send_to
    if len(data) == 1 and len(send_to) > 1:
        data = data * len(send_to)
    assert len(data) == len(send_to) or len(send_to) == 0

    # Let N be the total # of workers and P be the # of processes for the
    # current invocation.

    # (1) Send messages to main processes.
    # If all-to-all, send N messages; 1 to each worker (via SQS or Pipe).
    # If gather, send 1 message to main worker (via SQS).
    # If scatter, send N messages if main worker and 0 otherwise (via SQS).
    for worker_idx in send_to:
        # Convert data to string and batch it
        data_str_batches = data[worker_idx if len(data) > 1 else 0]

        # Iterate through each batch and send a message
        num_chunks = len(data_str_batches)
        for chunk_idx, data_str_batch in enumerate(data_str_batches):
            message = {
                "data_str": data_str_batch,
                "src_worker_idx": curr_worker_idx,
                "dst_worker_idx": worker_idx,
                "chunk_idx": chunk_idx,
                "num_chunks": num_chunks,
            }

            # Send to the main process of the worker this message is being sent
            # to.
            if is_same_invocation(curr_worker_idx, worker_idx):
                get_pipe(worker_idx).send(message)
            else:
                SQS.send_message(
                    QueueUrl=get_queue_url(worker_idx),
                    MessageBody=json.dumps(message),
                    # We set this ID using the source and destination worker
                    # indices so that they can be processed out of order.
                    MessageGroupId=f"{curr_worker_idx}-{worker_idx}",
                    MessageDeduplicationId=f"{num_messages_sent}-{curr_worker_idx}-{worker_idx}",
                )
        num_messages_sent += num_chunks

    # Dictionaries for receiving results
    num_chunks_needed = {}
    received_chunks = {}

    # (2) If main process, receive and send messages to destinations.

    if is_main_process(curr_worker_idx):
        # If all-to-all, receive P*P messages via Pipe and send to
        # destination worker via Pipe.
        # If gather, receive P messages via Pipe if main worker.
        # If scatter, receive P messages via Pipe and send to
        # destination worker via Pipe if main invocation.

        # Get destination process indices
        src_process_indices = []
        dst_process_indices = []
        if pattern == "shuffle":
            # We skip the 0th process (the main process) because that process
            # would have directly sent its messages to the other processes.
            src_process_indices = range(1, get_num_processes())
            dst_process_indices = range(get_num_processes())
        elif pattern == "gather" and is_main_worker(curr_worker_idx):
            src_process_indices = range(get_num_processes())
            dst_process_indices = [0]
        
        # Iterate over P workers (same-invocation processes) to receive from.
        for src_process_idx in src_process_indices:
            num_chunks_remaining = {}
            while any(
                num_chunks_remaining.get(dst_process_idx, -1) != 0
                for dst_process_idx in dst_process_indices
            ):
                # Receive message
                message = get_pipe(src_process_idx).recv()
                src_worker_idx = message["src_worker_idx"]
                dst_worker_idx = message["dst_worker_idx"]
                chunk_idx = message["chunk_idx"]
                dst_process_idx = get_process_idx(dst_worker_idx)
                if dst_process_idx not in num_chunks_remaining:
                    num_chunks_remaining[dst_process_idx] = message["num_chunks"]
                    num_chunks_needed[dst_worker_idx] = message["num_chunks"]
                num_chunks_remaining[dst_process_idx] -= 1

                # Forward or store received message
                if pattern == "shuffle":
                    get_pipe(dst_process_idx).send(message)
                elif pattern == "gather":
                    received_chunks[
                        (src_worker_idx, chunk_idx)
                    ] = message["data_str"]

        # If all-to-all, receive (N-P)*P messages from other workers not in the current
        # invocation (P messages per each of the N-P workers).
        # If gather, receive N-P messages via SQS if main worker.
        # If scatter, receive P messages (via SQS) and send to each
        # process (via Pipe) if not main invocation.

        # Get destination process indices
        src_worker_indices = []
        dst_process_indices = []
        if pattern == "shuffle":
            src_worker_indices = range(get_num_workers())
            dst_process_indices = range(get_num_processes())
        elif pattern == "gather":
            src_worker_indices = range(get_num_workers())
            dst_process_indices = [0]
        elif pattern == "scatter":
            src_worker_indices = [0]
            dst_process_indices = range(get_num_processes())

        curr_queue_url = get_queue_url(curr_worker_idx)
        num_chunks_remaining = {}
        already_received = set()
        receiving_from = [
            worker_idx
            for worker_idx in src_worker_indices
            if not is_same_invocation(worker_idx, curr_worker_idx)
        ]
        while any(
            num_chunks_remaining.get((worker_idx, dst_process_idx), -1) != 0
            for worker_idx in receiving_from
            for dst_process_idx in dst_process_indices
        ):
            resp = SQS.receive_message(QueueUrl=curr_queue_url)
            for msg in resp["Messages"]:
                msg_body = msg["Body"]
                msg = json.loads(msg_body)
                src_worker_idx = msg["src_worker_idx"]
                dst_worker_idx = msg["dst_worker_idx"]
                chunk_idx = msg["chunk_idx"]
                dst_process_idx = get_process_idx(dst_worker_idx)

                # Skip if already processed
                k = (dst_worker_idx, chunk_idx)
                if k in already_received:
                    continue
                already_received.add(k)

                # Send to destination process
                if pattern == "shuffle" or pattern == "scatter":
                    get_pipe(dst_worker_idx).send(msg)
                elif pattern == "gather":
                    received_chunks[
                        (src_worker_idx, chunk_idx)
                    ] = msg["data_str"]

                # Update # of chunks remaining
                key = (src_worker_idx, dst_process_idx)
                if key not in num_chunks_remaining:
                    num_chunks_remaining[key] = msg["num_chunks"]
                    num_chunks_needed[dst_worker_idx] = msg["num_chunks"]
                num_chunks_remaining[key] -= 1

    # (3) Receive messages from main process.
    # If all-to-all, receive N messages from main processes.
    # If gather, do nothing.
    # If scatter, receive 1 message (via Pipe).
    
    src_worker_indices = []
    if pattern == "shuffle":
        src_worker_indices = range(get_num_workers())
    elif pattern == "scatter":
        src_worker_indices = [0]

    num_chunks_remaining = {}
    while any(
        num_chunks_remaining.get(worker_idx, -1) != 0
        for worker_idx in src_worker_indices
    ):
        message = get_pipe(curr_worker_idx).recv()
        src_worker_idx = message["src_worker_idx"]
        if src_worker_idx not in num_chunks_remaining:
            num_chunks_remaining[src_worker_idx] = message["num_chunks"]
            num_chunks_needed[src_worker_idx] = message["num_chunks"]
            num_chunks_remaining[src_worker_idx] -= 1
        received_chunks[(src_worker_idx, chunk_idx)] = message["data_str"]

    # Consolidate received message chunks, deserialize, and return
    results = []
    for src_worker_idx in range(get_num_workers()):
        num_chunks = num_chunks_needed[src_worker_idx]
        data_str = ""
        for chunk_idx in range(num_chunks):
            data_str += received_chunks[(src_worker_idx, chunk_idx)]
        data_obj = from_str(data_str)
        results.append(data_obj)

    return results

    # TODO: Implement gather/scatter
    # TODO: Implement tests and get it to pass
    # TODO: Finish above and ensure that num_chunks_remaining is maintained correctly and we are getting all the messages
    # TODO: multithreading for parallelization - launch separate threads for different worker idx and only delete if thread can process
    # TODO: Eliminate redundant messages to self - add if statements to above algorithms and store in variables
    # TODO: Ensure each SQS message is unique - have a unique identifier
    # TODO: scatter and gather support - part of it
    # TODO: helper functions to use in send_to_client/receive_to_client - not needed
    
    raise NotImplementedError()


def sync():
    send_receive(0, "gather")
    send_receive(0, "scatter")


def send_to_client(data: Any):
    global num_messages_sent
    
    # Stringify and batch each piece of data
    from .constants import MAX_SQS_MESSAGE_LENGTH 
    data = list(batch(to_str(data), MAX_SQS_MESSAGE_LENGTH))
    num_chunks = len(data)

    # Send each chunk in a separate message
    SQS = sqs()
    for chunk_idx, data_str_batch in enumerate(data):
        message = {
            "data_str": data_str_batch,
            "src_worker_idx": 0,
            "dst_worker_idx": -1,
            "chunk_idx": chunk_idx,
            "num_chunks": num_chunks,
        }

        SQS.send_message(
            QueueUrl=get_queue_url("client"),
            MessageBody=json.dumps(message),
            # We set this ID using the chunk index so that messages are sent
            # out of order
            MessageGroupId=f"0-client-{chunk_idx}",
            MessageDeduplicationId=f"{num_messages_sent}-0-client",
        )
    num_messages_sent += num_chunks


def receive_to_client():
    SQS = sqs()

    num_chunks_remaining = -1
    already_received = set()
    received_chunks = {}
    while num_chunks_remaining != 0:
        resp = SQS.receive_message(QueueUrl=get_queue_url("client"))
        for msg in resp["Messages"]:
            msg_body = msg["Body"]
            msg = json.loads(msg_body)
            chunk_idx = msg["chunk_idx"]

            # Skip if already processed
            if chunk_idx in already_received:
                continue
            already_received.add(chunk_idx)

            # Store chunks
            received_chunks[chunk_idx] = msg["data_str"]

            # Update # of chunks remaining
            if num_chunks_remaining == -1:
                num_chunks_remaining = msg["num_chunks"]
            num_chunks_remaining -= 1
    
    # Deserialize and return the result
    data_str = "".join(
        [received_chunks[i] for i in range(len(received_chunks))]
    )
    return from_str(data_str)

