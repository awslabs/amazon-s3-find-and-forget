"""
Job Status Updater
"""
import os

import boto3

ddb = boto3.resource("dynamodb")
table = ddb.Table(os.getenv("JobTable", "S3F2_Jobs"))

status_map = {
    "QueryFailed": "FAILED",
    "ObjectUpdateFailed": "FAILED",
    "Exception": "FAILED",
    "JobStarted": "RUNNING",
    "JobSucceeded": "COMPLETED",
}


def update_status(event):
    event_name = event["EventName"]
    if event_name not in status_map:
        pass
    job_id = event["JobId"]
    status = status_map[event_name]
    table.update_item(
        Key={
            'Id': job_id,
            'Type': 'Job'
        },
        UpdateExpression="set #status = :s",
        ExpressionAttributeNames={
            '#status': 'JobStatus',
        },
        ExpressionAttributeValues={
            ':s': status,
        },
        ReturnValues="UPDATED_NEW"
    )
