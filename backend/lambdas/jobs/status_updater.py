"""
Job Status Updater
"""
import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ddb = boto3.resource("dynamodb")
table = ddb.Table(os.getenv("JobTable", "S3F2_Jobs"))

status_map = {
    "QueryFailed": "ABORTED",
    "ObjectUpdateFailed": "COMPLETED_WITH_ERRORS",
    "Exception": "FAILED",
    "JobStarted": "RUNNING",
    "JobSucceeded": "COMPLETED",
}

unlocked_states = ["RUNNING", "QUEUED"]

time_events = {
    "JobStarted": "JobStartTime",
    "JobSucceeded": "JobFinishTime",
}


def update_status(job_id, events):
    attr_updates = {}
    for event in events:
        # Ignore non status events
        event_name = event["EventName"]
        if event_name not in status_map:
            continue

        new_status = status_map[event_name]
        # Only change the status if it's still in an unlocked state
        if not attr_updates.get("JobStatus") or attr_updates.get("JobStatus") in unlocked_states:
            attr_updates["JobStatus"] = new_status

        # Update any job time events
        if event_name in time_events and not attr_updates.get(time_events[event_name]):
            attr_updates[time_events[event_name]] = event["EventData"]

    if len(attr_updates) > 0:
        _update_item(job_id, attr_updates)


def _update_item(job_id, attr_updates):
    try:
        update_expression = "set " + ", ".join(["#{k} = :{k}".format(k=k) for k, v in attr_updates.items()])
        attr_names = {}
        attr_values = {}

        for k, v in attr_updates.items():
            attr_names["#{}".format(k)] = k
            attr_values[":{}".format(k)] = v

        table.update_item(
            Key={
                'Id': job_id,
                'Sk': job_id,
            },
            UpdateExpression=update_expression,
            ConditionExpression="#JobStatus = :r OR #JobStatus = :q",
            ExpressionAttributeNames={
                "#JobStatus": "JobStatus",
                **attr_names
            },
            ExpressionAttributeValues={
                ':r': "RUNNING",
                ':q': "QUEUED",
                **attr_values,
            },
            ReturnValues="UPDATED_NEW"
        )
        logger.info("Updated Status for Job ID {}: {}".format(job_id, json.dumps(attr_updates)))
    except ddb.meta.client.exceptions.ConditionalCheckFailedException:
        logger.warning("Job {} is already in a status which cannot be updated".format(job_id))
