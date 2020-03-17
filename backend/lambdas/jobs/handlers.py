"""
Job handlers
"""
import json
import os

import boto3
from boto3.dynamodb.conditions import Key, Attr

from boto_utils import DecimalEncoder, utc_timestamp
from decorators import with_logging, request_validator, catch_errors, add_cors_headers, load_schema

ddb = boto3.resource("dynamodb")
table = ddb.Table(os.getenv("JobTable", "S3F2_Jobs"))
index = os.getenv("JobTableDateGSI", "Date-GSI")
bucket_count = int(os.getenv("GSIBucketCount", 1))

end_events = [
    "CleanupSucceeded", "CleanupFailed", "CleanupSkipped"
]


@with_logging
@add_cors_headers
@request_validator(load_schema("get_job"))
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


@with_logging
@add_cors_headers
@request_validator(load_schema("list_jobs"))
@catch_errors
def list_jobs_handler(event, context):
    qs = event.get("queryStringParameters")
    if not qs:
        qs = {}
    page_size = int(qs.get("page_size", 10))
    start_at = int(qs.get("start_at", utc_timestamp()))

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


@with_logging
@add_cors_headers
@request_validator(load_schema("list_job_events"))
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

    watermark_boundary_mu = job.get("JobFinishTime", utc_timestamp()) * 1000

    qs = event.get("queryStringParameters")
    if not qs:
        qs = {}
    page_size = int(qs.get("page_size", 20))
    start_at = qs.get("start_at", "0")

    # Check the watermark is not "future"
    if int(start_at.split("#")[0]) > watermark_boundary_mu:
        raise ValueError("Watermark {} is out of bounds for this job".format(start_at))

    # Because result may contain both JobEvent and Job items, we request page_size+1 items then apply the type
    # filter as FilterExpression. We then limit the list size to the requested page size in case the number of
    # items after filtering is still page_size+1 i.e. the Job item wasn't on the page.
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
