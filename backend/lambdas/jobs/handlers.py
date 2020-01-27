"""
Job handlers
"""
from datetime import datetime, timezone
import json
import os

import boto3
from aws_xray_sdk.core import xray_recorder
from boto3.dynamodb.conditions import Key, Attr

from boto_utils import DecimalEncoder
from decorators import with_logger, request_validator, catch_errors, load_schema, add_cors_headers

ddb = boto3.resource("dynamodb")
table = ddb.Table(os.getenv("JobTable", "S3F2_Jobs"))
index = os.getenv("JobTableDateGSI", "Date-GSI")
bucket_count = int(os.getenv("GSIBucketCount", 1))

end_events = ["FindPhaseFailed", "ForgetPhaseFailed", "Exception", "JobSucceeded"]


@with_logger
@xray_recorder.capture('GetJobHandler')
@add_cors_headers
@request_validator(load_schema("get_job_handler"), "pathParameters")
@catch_errors
def get_job_handler(event, context):
    job_id = event["pathParameters"]["job_id"]
    resp = table.get_item(
        Key={
            'Id': job_id,
            'Sk': job_id,
        }
    )
    item = resp.get('Item')
    if not item:
        return {
            "statusCode": 404
        }

    return {
        "statusCode": 200,
        "body": json.dumps(item, cls=DecimalEncoder)
    }


@with_logger
@xray_recorder.capture('ListJobsHandler')
@add_cors_headers
@catch_errors
def list_jobs_handler(event, context):
    qs = event.get("queryStringParameters")
    if not qs:
        qs = {}
    page_size = int(qs.get("page_size", 10))
    start_at = int(qs.get("start_at", round(datetime.now(timezone.utc).timestamp())))

    items = []
    for gsi_bucket in range(0, bucket_count):
        response = table.query(
            IndexName=index,
            KeyConditionExpression=Key('GSIBucket').eq(str(gsi_bucket)) & Key('CreatedAt').lt(start_at),
            ScanIndexForward=False,
            Limit=page_size,
        )
        items += response.get("Items", [])
    items = sorted(items, key=lambda i: i['CreatedAt'], reverse=True)[:page_size]
    if len(items) < page_size:
        next_start = None
    else:
        next_start = min([item["CreatedAt"] for item in items])

    return {
        "statusCode": 200,
        "body": json.dumps({
            "Jobs": items,
            "NextStart": next_start,
        }, cls=DecimalEncoder)
    }


@with_logger
@xray_recorder.capture('ListJobEventsHandler')
@add_cors_headers
@catch_errors
def list_job_events_handler(event, context):
    job_id = event["pathParameters"]["job_id"]
    # Check the job exists
    job = table.get_item(
        Key={
            'Id': job_id,
            'Sk': job_id,
        }
    )

    watermark_boundary = job.get("JobFinishTime", round(datetime.now(timezone.utc).timestamp())) * 1000  # microseconds

    qs = event.get("queryStringParameters")
    if not qs:
        qs = {}
    page_size = int(qs.get("page_size", 20))
    start_at = qs.get("start_at", "0")

    # Check the watermark is not "future"
    if int(start_at.split("#")[0]) > watermark_boundary:
        raise ValueError("Watermark {} is out of bounds for this job".format(start_at))

    # Check the watermark is not "future"
    results = table.query(
        KeyConditionExpression=Key('Id').eq(job_id),
        ScanIndexForward=True,
        Limit=page_size + 1,
        FilterExpression=Attr('Type').eq("JobEvent"),
        ExclusiveStartKey={
            "Id": job_id,
            "Sk": str(start_at)
        }
    )

    items = results.get("Items", [])[:page_size]

    resp = {
        "JobEvents": items
    }
    if len(items) > 0:
        if not any([i["EventName"] in end_events for i in items]):
            resp["NextStart"] = items[len(items) - 1]["Sk"]
    else:
        resp["NextStart"] = start_at

    return {
        "statusCode": 200,
        "body": json.dumps(resp, cls=DecimalEncoder)
    }
