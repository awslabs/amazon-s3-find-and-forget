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


def update_status(event):
    event_name = event["EventName"]
    if event_name not in status_map:
        pass
    job_id = event["Id"]
    status = status_map[event_name]
    try:
        table.update_item(
            Key={
                'Id': job_id,
                'Sk': job_id,
            },
            UpdateExpression="set #status = :s",
            ConditionExpression="#status = :r OR #status = :c OR #status = :q",
            ExpressionAttributeNames={
                '#status': 'JobStatus',
            },
            ExpressionAttributeValues={
                ':s': status,
                ':r': "RUNNING",
                ':c': "COMPLETED",
                ':q': "QUEUED",
            },
            ReturnValues="UPDATED_NEW"
        )
    except ddb.meta.client.exceptions.ConditionalCheckFailedException:
        logger.warning("Job {} is already in a status which cannot be updated".format(job_id))
