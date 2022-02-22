import boto3
import json

from id import generate_message_id, ResourceId 
from sessions import get_session, get_session_id, end_session

sqs = boto3.client('sqs') 

#################
# GET QUEUE URL #
#################

def get_scatter_queue(resource_id = None):
    if resource_id is None:
        resource_id = get_session().resource_id
    return sqs.get_queue_url(QueueName="banyan_" + resource_id + "_scatter.fifo")["QueueUrl"]

def get_gather_queue(resource_id = None):
    if resource_id is None:
        resource_id = get_session().resource_id
    return sqs.get_queue_url(QueueName="banyan_" + resource_id + "_gather.fifo")["QueueUrl"]

def get_execution_queue(resource_id = None):
    if resource_id is None:
        resource_id = get_session().resource_id
    return sqs.get_queue_url(QueueName="banyan_" + resource_id + "_execution.fifo")["QueueUrl"]

###################
# RECEIVE MESSAGE #
###################

def get_next_message(queue, delete = True):
    message_receiving_result = sqs.receive_message(QueueUrl = queue, MaxNumberOfMessages=1)
    m = message_receiving_result["Messages"][0]["ReceiptHandle"] if len(message_receiving_result["Messages"]) > 0 else None
    
    while m is None:
        message_receiving_result = sqs.receive_message(QueueUrl = queue, MaxNumberOfMessages=1)
        m = message_receiving_result["Messages"][0]["ReceiptHandle"] if len(message_receiving_result["Messages"]) > 0 else None
      
    if delete:
        sqs.delete_message(QueueUrl = queue, ReceiptHandle = m)       
    return message_receiving_result["Messages"][0]["Body"]

def receive_next_message(queue_name):
    content = get_next_message(queue_name)  

    if content.startswith("JOB_READY") or content.startswith("SESSION_READY"):
        response = {"kind": "SESSION_READY"}
        return response
    elif content.startswith("EVALUATION_END"):
        response = {"kind": "EVALUATION_END"}
        response["end"] = content.endswith("MESSAGE_END")
        print(content[14:-1])
        return response
    elif content.startswith("JOB_FAILURE") or content.startswith(content, "SESSION_FAILURE"):
        tail = 11 if content.endswith("MESSAGE_END") else 0
        head_len = 11 if content.startswith("JOB_FAILURE") else 15
        print(content[head_len:tail])
        if content.endswith("MESSAGE_END"):
            end_session(failed = True, release_resources_now = content.startswith("JOB_FAILURE"))
            raise RuntimeError("Session failed; see preceding output") 
        response = {"kind": "SESSION_FAILURE"}
        return response 
    else:
        return json.loads(content)

# Used by Banyan/src/pfs.jl, intended to be called from the executor
def receive_from_client(value_id):
    # Send scatter message to client
    send_message(
        get_gather_queue(),
        json.dumps({"kind": "SCATTER_REQUEST", "value_id": value_id})
    )
    # Receive response from client
    m = json.loads(get_next_message(get_scatter_queue()))
    # TODO: Implement from_jl_value_contents
    # v = from_jl_value_contents(m["contents"])
    v = None
    return v


################
# SEND MESSAGE #
################

def send_message(queue_name, message):
    queue_url = sqs.get_queue_url(queue_name)
    return sqs.send_message(
        QueueUrl = queue_url,
        MessageBody=message, # TODO: Is that correct?,
        MessageGroupId = "1",
        MessageDuplicationId = generate_message_id() # TODO: where does that function come from?
    )

def send_to_client(value_id, value):
    send_message(
        get_gather_queue()
        json.dumps(
            {
                "kind":"GATHER",
                "value_id":value_id,
                "contents":to_jl_value_contents(value)
            }
        )

###########################
# GET MESSAGES FROM QUEUE #
###########################




##########################
# SEND MESSAGES TO QUEUE #
##########################