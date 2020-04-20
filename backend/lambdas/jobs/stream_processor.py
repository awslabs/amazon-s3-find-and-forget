import logging
from datetime import datetime, timezone
from os import getenv
import json
import boto3
from boto3.dynamodb.types import TypeDeserializer
from itertools import groupby
from operator import itemgetter

from stats_updater import update_stats
from status_updater import update_status, skip_cleanup_states
from boto_utils import DecimalEncoder, deserialize_item, emit_event, utc_timestamp
from decorators import with_logging

deserializer = TypeDeserializer()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('stepfunctions')
state_machine_arn = getenv("StateMachineArn")
ddb = boto3.resource("dynamodb")
q_table = ddb.Table(getenv("DeletionQueueTable"))


@with_logging
def handler(event, context):
    records = event["Records"]
    new_jobs = [
        deserialize_item(r["dynamodb"]["NewImage"]) for r in records
        if is_record_type(r, "Job") and is_operation(r, "INSERT")
    ]
    events = [
        deserialize_item(r["dynamodb"]["NewImage"]) for r in records
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
        if updated_job and updated_job.get("JobStatus") == "FORGET_COMPLETED_CLEANUP_IN_PROGRESS":
            try:
                clear_deletion_queue(updated_job)
                emit_event(job_id, "CleanupSucceeded", utc_timestamp(), "StreamProcessor")
            except Exception as e:
                emit_event(job_id, "CleanupFailed", {
                    "Error": "Unable to clear deletion queue: {}".format(str(e))
                }, "StreamProcessor")
        elif updated_job and updated_job.get("JobStatus") in skip_cleanup_states:
            emit_event(job_id, "CleanupSkipped", utc_timestamp(), "StreamProcessor")


def process_job(job):
    job_id = job["Id"]
    try:
        client.start_execution(
            stateMachineArn=state_machine_arn,
            name=job_id,
            input=json.dumps(job, cls=DecimalEncoder)
        )
    except client.exceptions.ExecutionAlreadyExists:
        logger.warning("Execution %s already exists", job_id)
    except Exception as e:
        emit_event(job_id, "Exception", {
            "Error": "Unable to start StepFunction execution: {}".format(str(e))
        }, "StreamProcessor")


def clear_deletion_queue(job):
    logger.info("Clearing successfully deleted matches")
    for item in job.get("DeletionQueueItems", []):
        with q_table.batch_writer() as batch:
            batch.delete_item(Key={
                "MatchId": item["MatchId"],
                "CreatedAt": item["CreatedAt"]
            })


def is_operation(record, operation):
    return record.get("eventName") == operation


def is_record_type(record, record_type):
    new_image = record["dynamodb"].get("NewImage")
    if not new_image:
        return False
    item = deserialize_item(new_image)
    return item.get("Type") and item.get("Type") == record_type
