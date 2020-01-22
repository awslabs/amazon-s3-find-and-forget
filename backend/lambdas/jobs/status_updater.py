"""
Job Status Updater
"""
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

time_events = {
    "JobStarted": "JobStartTime",
    "JobSucceeded": "JobFinishTime"
}


def update_status(job_id, events):
    status = None
    for event in events:
        status = _update_status(job_id, event)

    return status


def _update_status(job_id, event):
    event_name = event["EventName"]
    if event_name not in status_map:
        return
    status = status_map[event_name]
    try:
        update_expression = "set #status = :s"
        additional_attr_names = {}
        additional_attr_values = {}
        if event_name in time_events:
            update_expression += ", #time_attr = :t"
            additional_attr_names["#time_attr"] = time_events[event_name]
            additional_attr_values[":t"] = event["EventData"]
        table.update_item(
            Key={
                'Id': job_id,
                'Sk': job_id,
            },
            UpdateExpression=update_expression,
            ConditionExpression="#status = :r OR #status = :c OR #status = :q",
            ExpressionAttributeNames={
                '#status': 'JobStatus',
                **additional_attr_names,
            },
            ExpressionAttributeValues={
                ':s': status,
                ':r': "RUNNING",
                ':c': "COMPLETED",
                ':q': "QUEUED",
                **additional_attr_values,
            },
            ReturnValues="UPDATED_NEW"
        )

        return status
    except ddb.meta.client.exceptions.ConditionalCheckFailedException:
        logger.warning("Job {} is already in a status which cannot be updated".format(job_id))
