import json
import os
import boto3

from decorators import with_logger
from boto_utils import read_queue

concurrency_limit = os.getenv("AthenaConcurrencyLimit", 20)
queue_url = os.getenv("QueueUrl")
wait_duration = os.getenv("WaitDuration", 15)
state_machine_arn = os.getenv("StateMachineArn")
sqs = boto3.resource("sqs")
sf_client = boto3.client("stepfunctions")
logs = boto3.client("logs")


@with_logger
def handler(event, context):
    execution_id = event["ExecutionId"]
    job_id = event["ExecutionName"]

    if any_query_has_failed(job_id):
        raise RuntimeError("One or more queries failed. Abandoning execution")

    queue = sqs.Queue(queue_url)
    not_visible = int(event["QueryQueue"]["NotVisible"])
    visible = int(event["QueryQueue"]["Visible"])
    limit = int(concurrency_limit)
    reamining_capacity = limit - not_visible
    to_process = min(reamining_capacity, visible)
    if to_process > 0:
        msgs = read_queue(queue, to_process)
        for msg in msgs:
            context.logger.debug(msg.body)
            # TODO: Handle message received multiple times
            body = json.loads(msg.body)
            body["AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID"] = execution_id
            body["JobId"] = job_id
            body["ReceiptHandle"] = msg.receipt_handle
            body["WaitDuration"] = wait_duration
            sf_client.start_execution(stateMachineArn=state_machine_arn, input=json.dumps(body))


def any_query_has_failed(job_id):
    log_group = os.getenv("LogGroupName", "/aws/s3f2")
    return len(logs.filter_log_events(
        logGroupName=log_group,
        logStreamNamePrefix=job_id,
        filterPattern='QueryFailed',
        limit=1
    ).get('events', [])) > 0

