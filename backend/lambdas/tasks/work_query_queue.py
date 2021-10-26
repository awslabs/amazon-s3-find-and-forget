import json
import os
import boto3

from decorators import with_logging, s3_state_store
from boto_utils import read_queue

queue_url = os.getenv("QueueUrl")
state_machine_arn = os.getenv("StateMachineArn")
sqs = boto3.resource("sqs")
queue = sqs.Queue(queue_url)
sf_client = boto3.client("stepfunctions")

QUERY_EXECUTION_MAX_RETRIES = 2


@with_logging
@s3_state_store(offload_keys=["Data"])
def handler(event, context):
    concurrency_limit = int(event.get("AthenaConcurrencyLimit", 15))
    wait_duration = int(event.get("QueryExecutionWaitSeconds", 15))
    execution_id = event["ExecutionId"]
    job_id = event["ExecutionName"]
    previously_started = event.get("RunningExecutions", {"Data": [], "Total": 0})
    executions = [load_execution(execution) for execution in previously_started["Data"]]
    succeeded = [
        execution for execution in executions if execution["status"] == "SUCCEEDED"
    ]
    still_running = [
        execution for execution in executions if execution["status"] == "RUNNING"
    ]
    failed = [
        execution
        for execution in executions
        if execution["status"] not in ["SUCCEEDED", "RUNNING"]
    ]
    clear_completed(succeeded)
    is_failing = previously_started.get("IsFailing", False)
    if len(failed) > 0:
        is_failing = True
    # Only abandon for failures once all running queries are done
    if is_failing and len(still_running) == 0:
        abandon_execution(failed)

    remaining_capacity = int(concurrency_limit) - len(still_running)
    # Only schedule new queries if there have been no errors
    if remaining_capacity > 0 and not is_failing:
        msgs = read_queue(queue, remaining_capacity)
        started = []
        for msg in msgs:
            body = json.loads(msg.body)
            body["AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID"] = execution_id
            body["JobId"] = job_id
            body["WaitDuration"] = wait_duration
            body["ExecutionRetriesLeft"] = QUERY_EXECUTION_MAX_RETRIES
            query_executor = body["QueryExecutor"]
            if query_executor == "athena":
                resp = sf_client.start_execution(
                    stateMachineArn=state_machine_arn, input=json.dumps(body)
                )
                started.append({**resp, "ReceiptHandle": msg.receipt_handle})
            else:
                raise NotImplementedError(
                    "Unsupported query executor: '{}'".format(query_executor)
                )
        still_running += started

    return {
        "IsFailing": is_failing,
        "Data": [
            {"ExecutionArn": e["executionArn"], "ReceiptHandle": e["ReceiptHandle"]}
            for e in still_running
        ],
        "Total": len(still_running),
    }


def load_execution(execution):
    resp = sf_client.describe_execution(executionArn=execution["ExecutionArn"])
    resp["ReceiptHandle"] = execution["ReceiptHandle"]
    return resp


def clear_completed(executions):
    for e in executions:
        message = sqs.Message(queue.url, e["ReceiptHandle"])
        message.delete()


def abandon_execution(failed):
    raise RuntimeError(
        "Abandoning execution because one or more queries failed. {}".format(
            ", ".join([f["executionArn"] for f in failed])
        )
    )
