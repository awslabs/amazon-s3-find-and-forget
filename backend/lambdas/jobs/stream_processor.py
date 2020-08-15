import logging
from datetime import datetime, timezone
from os import getenv
import json
import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from itertools import groupby
from operator import itemgetter

from stats_updater import update_stats
from status_updater import update_status, skip_cleanup_states, final_statuses
from boto_utils import DecimalEncoder, deserialize_item, emit_event, utc_timestamp
from decorators import with_logging

deserializer = TypeDeserializer()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client("stepfunctions")
state_machine_arn = getenv("StateMachineArn")
ddb = boto3.resource("dynamodb")
q_table = ddb.Table(getenv("DeletionQueueTable"))
s3 = boto3.resource("s3")
eraser_head_lambda = getenv("EraserHeadLambda")


@with_logging
def handler(event, context):
    records = event["Records"]
    new_jobs = [
        deserialize_item(r["dynamodb"]["NewImage"])
        for r in records
        if is_record_type(r, "Job") and is_operation(r, "INSERT")
    ]
    events = [
        deserialize_item(r["dynamodb"]["NewImage"])
        for r in records
        if is_record_type(r, "JobEvent") and is_operation(r, "INSERT")
    ]
    grouped_events = groupby(sorted(events, key=itemgetter("Id")), key=itemgetter("Id"))

    for job in new_jobs:
        process_job(job)

    for job_id, group in grouped_events:
        group = [i for i in group]
        update_stats(job_id, group)
        updated_job = update_status(job_id, group)
        # Perform cleanup if required
        if (
            updated_job
            and updated_job.get("JobStatus") == "FORGET_COMPLETED_CLEANUP_IN_PROGRESS"
        ):
            try:
                clear_deletion_queue(updated_job)
                emit_event(
                    job_id, "CleanupSucceeded", utc_timestamp(), "StreamProcessor"
                )
            except Exception as e:
                emit_event(
                    job_id,
                    "CleanupFailed",
                    {"Error": "Unable to clear deletion queue: {}".format(str(e))},
                    "StreamProcessor",
                )
        if updated_job and updated_job.get("JobStatus") in final_statuses:
            lambda_client = boto3.client('lambda', 'us-west-2')
            p = {
                "neura_env": updated_job.get("NeuraEnv", "staging"),
                "data_stores": "s3_verification"
            }
            try:
                lambda_client.invoke_async(FunctionName=eraser_head_lambda, InvokeArgs=json.dumps(p))
                logger.info("Successfully invoked {} Lambda".format(eraser_head_lambda))
            except Exception:
                logger.exception("Couldnt start eraserHead Lambda")


def process_job(job):
    job_id = job["Id"]
    state = {
        k: job[k]
        for k in [
            "AthenaConcurrencyLimit",
            "DeletionTasksMaxNumber",
            "ForgetQueueWaitSeconds",
            "Id",
            "QueryExecutionWaitSeconds",
            "NeuraEnv",
            "QueryQueueWaitSeconds",
        ]
    }

    try:
        client.start_execution(
            stateMachineArn=state_machine_arn,
            name=job_id,
            input=json.dumps(state, cls=DecimalEncoder),
        )
    except client.exceptions.ExecutionAlreadyExists:
        logger.warning("Execution %s already exists", job_id)
    except (ClientError, ValueError) as e:
        emit_event(
            job_id,
            "Exception",
            {
                "Error": "ExecutionFailure",
                "Cause": "Unable to start StepFunction execution: {}".format(str(e)),
            },
            "StreamProcessor",
        )


def clear_deletion_queue(job):
    logger.info("Clearing successfully deleted matches")
    deletion_queue_bucket = job.get("DeletionQueueBucket")
    deletion_queue_key = job.get("DeletionQueueKey")
    obj = s3.Object(deletion_queue_bucket, deletion_queue_key)
    raw_data = obj.get()['Body'].read().decode('utf-8')
    data = json.loads(raw_data)
    deletion_queue_items = data["DeletionQueueItems"]
    with q_table.batch_writer() as batch:
        for item in deletion_queue_items:
            batch.delete_item(Key={"DeletionQueueItemId": item["DeletionQueueItemId"]})


def is_operation(record, operation):
    return record.get("eventName") == operation


def is_record_type(record, record_type):
    new_image = record["dynamodb"].get("NewImage")
    if not new_image:
        return False
    item = deserialize_item(new_image)
    return item.get("Type") and item.get("Type") == record_type
