"""
Job handlers
"""
import datetime
import json
import os

import boto3
from aws_xray_sdk.core import xray_recorder
from boto3.dynamodb.conditions import Key

from boto_utils import DecimalEncoder
from decorators import with_logger, request_validator, catch_errors, load_schema, add_cors_headers

ddb = boto3.resource("dynamodb")
table = ddb.Table(os.getenv("JobTable", "S3F2_Jobs"))
index = os.getenv("JobTableDateGSI", "Date-GSI")
bucket_count = int(os.getenv("GSIBucketCount", 1))


@with_logger
@xray_recorder.capture('GetJobHandler')
@add_cors_headers
@request_validator(load_schema("get_job_handler"), "pathParameters")
@catch_errors
def get_job_handler(event, context):
    job_id = event["pathParameters"]["job_id"]
    resp = table.get_item(
        Key={
            'JobId': job_id
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
    start_at = int(qs.get("start_at", round(datetime.datetime.now().timestamp())))

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

