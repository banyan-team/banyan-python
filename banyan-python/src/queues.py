import boto3
import json


sqs = boto3.client('sqs') # TODO: get configuration

def get_scatter_queue(job_id: JobId):
    return sqs.get_queue_url(QueueName="banyan_" + job_id + "_scatter.fifo")

def get_gather_queue(job_id: JobId):
    return sqs.get_queue_url(QueueName="banyan_" + job_id + "_gather.fifo")

def receive_next_message(queue_name):
    global jobs

    queue_url = sqs.get_queue_url(queue_name)
    job_id = get_job_id() # TODO: fix. I don't know where get_job_id comes from

    m = sqs.receive_message( # TODO: use WaitTimeSeconds?
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
    )

    while len(m) == 0:
        m = sqs.receive_message( # TODO: use WaitTimeSeconds?
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
        )
    content = m["Messages"][0]["Body"]
    handle = m["Messages"][0]["ReceiptHandle"]

    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=handle)

    if content.startswith("EVALUATION_END"):
        print(content[15:])
        response: dict[str, any] = {"kind": "EVALUATION_END"}
        response["end"] = content.endswith("MESSAGE_END")

        return response
    elif content.startswith("JOB_FAILURE"):
        jobs[job_id].current_status = "failed"
        print(content[12:])
        if content.endswith("MESSAGE_END"):
            raise RuntimeError("Job failed; see preceding output") # TODO: what kind of error should be raised?
        response: dict[str, any] = {"kind": "JOB_FAILURE"}
        return response # TODO: is that what we want to return?
    else:
        return json.loads(content)

def send_message(queue_name, message):
    queue_url = sqs.get_queue_url(queue_name)
    return sqs.send_message(
        QueueUrl = queue_url,
        MessageBody=message, # TODO: Is that correct?,
        MessageGroupId = "1",
        MessageDuplicationId = generate_message_id() # TODO: where does that function come from?
    )