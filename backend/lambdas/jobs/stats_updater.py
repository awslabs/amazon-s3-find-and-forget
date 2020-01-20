"""
Job Stats Updater
"""
import os

import boto3

ddb = boto3.resource("dynamodb")
table = ddb.Table(os.getenv("JobTable", "S3F2_Jobs"))


def update_stats(event):
    job_id = event["JobId"]
    event_name = event["EventName"]
    event_data = event.get("EventData", {})
    if event_name in ["QuerySucceeded", "QueryFailed"]:
        _update_query_stats(job_id, event_name, event_data)
    if event_name in ["ObjectUpdated", "ObjectUpdateFailed"]:
        _update_object_stats(job_id, event_name)


def _update_query_stats(job_id, event_name, event_data):
    table.update_item(
        Key={
            'Id': job_id,
            'Type': 'Job'
        },
        UpdateExpression="set #q = if_not_exists(#q, :z) + :q, "
                         "#qs = if_not_exists(#qs, :z) + :qs, "
                         "#f = if_not_exists(#f, :z) + :f, "
                         "#s = if_not_exists(#s, :z) + :s, "
                         "#t = if_not_exists(#t, :z) + :t",
        ExpressionAttributeNames={
            '#q': 'TotalQueryCount',
            '#qs': 'TotalQuerySucceededCount',
            '#f': 'TotalQueryFailedCount',
            '#s': 'TotalQueryScannedInBytes',
            '#t': 'TotalQueryTimeInMillis',
        },
        ExpressionAttributeValues={
            ':q': 1,
            ':qs': 1 if event_name == "QuerySucceeded" else 0,
            ':f': 1 if event_name == "QueryFailed" else 0,
            ':s': event_data.get("QueryStatus", {}).get("Statistics", {}).get("DataScannedInBytes", 0),
            ':t': event_data.get("QueryStatus", {}).get("Statistics", {}).get("EngineExecutionTimeInMillis", 0),
            ':z': 0,
        },
        ReturnValues="UPDATED_NEW"
    )


def _update_object_stats(job_id, event_name):
    table.update_item(
        Key={
            'Id': job_id,
            'Type': 'Job'
        },
        UpdateExpression="set #c = if_not_exists(#c, :z) + :c, #f = if_not_exists(#f, :z) + :f",
        ExpressionAttributeNames={
            '#c': 'TotalObjectUpdatedCount',
            '#f': 'TotalObjectUpdateFailedCount',
        },
        ExpressionAttributeValues={
            ':c': 1 if event_name == "ObjectUpdated" else 0,
            ':f': 1 if event_name == "ObjectUpdateFailed" else 0,
            ':z': 0,
        },
        ReturnValues="UPDATED_NEW"
    )

{"Id":{"S": "5a826122-e505-4a05-8d0b-f99a99df4a76"},"Type":{"S":"Job"}}