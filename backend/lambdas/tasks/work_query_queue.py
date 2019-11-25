import json
import os
import boto3

from decorators import with_logger
from boto_utils import read_queue

concurrency_limit = os.getenv("AthenaConcurrencyLimit", 20)
queue_url = os.getenv("QueueUrl")
state_machine_arn = os.getenv("StateMachineArn")
sqs = boto3.resource("sqs")
sf_client = boto3.client("stepfunctions")


@with_logger
def handler(event, context):
    queue = sqs.Queue(queue_url)
    not_visible = int(event["QueryQueue"]["NotVisible"])
    limit = int(concurrency_limit)
    remaining_capacity = limit - not_visible
    if remaining_capacity > 0:
        msgs = read_queue(queue, remaining_capacity)
        for msg in msgs:
            context.logger.debug(msg.body)
            # TODO: Handle message received multiple times
            body = json.loads(msg.body)
            body["ReceiptHandle"] = msg.receipt_handle
            sf_client.start_execution(stateMachineArn=state_machine_arn, input=json.dumps(body))
