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
from status_updater import update_status, skip_cleanup_states
from boto_utils import (
    DecimalEncoder,
    deserialize_item,
    emit_event,
    fetch_job_manifest,
    json_lines_iterator,
    utc_timestamp,
)
from decorators import with_logging

deserializer = TypeDeserializer()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client("stepfunctions")
ddb = boto3.resource("dynamodb")
glue = boto3.client("glue")

state_machine_arn = getenv("StateMachineArn")
q_table = ddb.Table(getenv("DeletionQueueTable"))
glue_db = getenv("GlueDatabase", "s3f2_manifests_database")
glue_table = getenv("JobManifestsGlueTable", "s3f2_manifests_table")


@with_logging
def handler(event, context):
    records = event["Records"]
    new_jobs = get_records(records, "Job", "INSERT")
    deleted_jobs = get_records(records, "Job", "REMOVE", new_image=False)
    events = get_records(records, "JobEvent", "INSERT")
    grouped_events = groupby(sorted(events, key=itemgetter("Id")), key=itemgetter("Id"))
    for job in new_jobs:
        process_job(job)

    for job in deleted_jobs:
        cleanup_manifests(job)

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
        elif updated_job and updated_job.get("JobStatus") in skip_cleanup_states:
            emit_event(job_id, "CleanupSkipped", utc_timestamp(), "StreamProcessor")


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


def cleanup_manifests(job):
    logger.info("Removing job manifest partitions")
    job_id = job["Id"]
    partitions = []
    for manifest in job.get("Manifests", []):
        data_mapper_id = manifest.split("/")[5]
        partitions.append([job_id, data_mapper_id])
    max_deletion_batch_size = 25
    for i in range(0, len(partitions), max_deletion_batch_size):
        glue.batch_delete_partition(
            DatabaseName=glue_db,
            TableName=glue_table,
            PartitionsToDelete=[
                {"Values": partition_tuple}
                for partition_tuple in partitions[i : i + max_deletion_batch_size]
            ],
        )


def clear_deletion_queue(job):
    logger.info("Clearing successfully deleted matches")
    to_delete = set()
    for manifest_object in job.get("Manifests", []):
        manifest = fetch_job_manifest(manifest_object)
        for line in json_lines_iterator(manifest):
            to_delete.add(line["DeletionQueueItemId"])

    with q_table.batch_writer() as batch:
        for item_id in to_delete:
            batch.delete_item(Key={"DeletionQueueItemId": item_id})


def is_operation(record, operation):
    return record.get("eventName") == operation


def is_record_type(record, record_type, new_image):
    image = record["dynamodb"].get("NewImage" if new_image else "OldImage")
    if not image:
        return False
    item = deserialize_item(image)
    return item.get("Type") and item.get("Type") == record_type


def get_records(records, record_type, operation, new_image=True):
    return [
        deserialize_item(r["dynamodb"].get("NewImage" if new_image else "OldImage", {}))
        for r in records
        if is_record_type(r, record_type, new_image) and is_operation(r, operation)
    ]
