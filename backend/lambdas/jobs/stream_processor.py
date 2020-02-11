import logging
from os import getenv
import json
import boto3
from boto3.dynamodb.types import TypeDeserializer
from itertools import groupby
from operator import itemgetter

from stats_updater import update_stats
from status_updater import update_status
from boto_utils import DecimalEncoder, deserialize_item, emit_event
from decorators import with_logger

deserializer = TypeDeserializer()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('stepfunctions')
state_machine_arn = getenv("StateMachineArn")
ddb = boto3.resource("dynamodb")
q_table = ddb.Table(getenv("DeletionQueueTable"))


@with_logger
def handler(event, context):
    records = event["Records"]
    new_jobs = [
        deserialize_item(r["dynamodb"]["NewImage"]) for r in records if is_job(r) and is_operation(r, "INSERT")
    ]
    events = [
        deserialize_item(r["dynamodb"]["NewImage"]) for r in records if is_job_event(r) and is_operation(r, "INSERT")
    ]
    grouped_events = groupby(sorted(events, key=itemgetter("Id")), key=itemgetter("Id"))

    for job in new_jobs:
        process_job(job)

    for job_id, group in grouped_events:
        group = [i for i in group]
        update_stats(job_id, group)
        updated_job = update_status(job_id, group)

        if updated_job and updated_job.get("JobStatus") == "FORGET_COMPLETED_CLEANUP_IN_PROGRESS":
            try:
                clear_deletion_queue(updated_job)
                emit_event(job_id, "CleanupSucceeded", {}, "StreamProcessor")
            except Exception as e:
                emit_event(job_id, "CleanupFailed", {
                    "Error": "Unable to clear deletion queue: {}".format(str(e))
                }, "StreamProcessor")


def process_job(job):
    job_id = job["Id"]
    try:
        client.start_execution(
            stateMachineArn=state_machine_arn,
            name=job_id,
            input=json.dumps(job, cls=DecimalEncoder)
        )
    except client.exceptions.ExecutionAlreadyExists:
        logger.warning("Execution {} already exists".format(job_id))


def clear_deletion_queue(job):
    logger.info("Clearing successfully deleted matches")
    for item in job.get("DeletionQueue", []):
        with q_table.batch_writer() as batch:
            batch.delete_item(Key={
                "MatchId": item["MatchId"],
                "CreatedAt": item["CreatedAt"]
            })


def is_operation(record, operation):
    return record.get("eventName") == operation


def is_job(record):
    item = deserialize_item(record["dynamodb"]["NewImage"])
    return item.get("Type") and item.get("Type") == "Job"


def is_job_event(record):
    item = deserialize_item(record["dynamodb"]["NewImage"])
    return item.get("Type") and item.get("Type") == "JobEvent"
