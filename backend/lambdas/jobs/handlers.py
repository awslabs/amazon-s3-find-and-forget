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

end_statuses = [
    "COMPLETED_CLEANUP_FAILED",
    "COMPLETED",
    "FAILED",
    "FIND_FAILED",
    "FORGET_FAILED",
    "FORGET_PARTIALLY_FAILED"
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
    # Input parsing
    job_id = event["pathParameters"]["job_id"]
    mvqs = event.get("multiValueQueryStringParameters", {})
    qs = event.get("queryStringParameters")
    if not qs:
        qs = {}
    page_size = int(qs.get("page_size", 20))
    start_at = qs.get("start_at", "0")
    # Check the job exists
    job = table.get_item(
        Key={
            'Id': job_id,
            'Sk': job_id,
        }
    ).get("Item")
    if not job:
        return {
            "statusCode": 404
        }

    watermark_boundary_mu = job.get("JobFinishTime", utc_timestamp()) * 1000

    # Check the watermark is not "future"
    if int(start_at.split("#")[0]) > watermark_boundary_mu:
        raise ValueError("Watermark {} is out of bounds for this job".format(start_at))

    # Apply filters
    filter_expression = Attr('Type').eq("JobEvent")
    user_filters = mvqs.get("filter", [])
    for f in user_filters:
        k, v = f.split("=")
        filter_expression = filter_expression & Attr(k).begins_with(v)

    # Because result may contain both JobEvent and Job items, we request max page_size+1 items then apply the type
    # filter as FilterExpression. We then limit the list size to the requested page size in case the number of
    # items after filtering is still page_size+1 i.e. the Job item wasn't on the page.
    items = []
    query_start_key = str(start_at)
    last_evaluated = None
    while len(items) < page_size:
        resp = table.query(
            KeyConditionExpression=Key('Id').eq(job_id),
            ScanIndexForward=True,
            FilterExpression=filter_expression,
            Limit=100 if len(user_filters) else page_size + 1,
            ExclusiveStartKey={
                "Id": job_id,
                "Sk": query_start_key
            }
        )
        results = resp.get("Items", [])
        items.extend(results[:page_size - len(items)])
        query_start_key = resp.get("LastEvaluatedKey", {}).get("Sk")
        if not query_start_key:
            break
        last_evaluated = query_start_key

    next_start = _get_watermark(items, start_at, page_size, job["JobStatus"], query_start_key, last_evaluated)

    resp = {k: v for k, v in {
        "JobEvents": items,
        "NextStart": next_start
    }.items() if v is not None}

    return {
        "statusCode": 200,
        "body": json.dumps(resp, cls=DecimalEncoder)
    }


def _get_watermark(items, initial_start_key, page_size, job_status, last_query_start_key, last_evaluated_ddb_key):
    """
    Work out the watermark to return to the user using the following logic:
    1. If the job is in progress. In this scenario we always return a watermark but the source of the watermark
       is determined as follows:
       a. We've cycled through multiple DDB pages the last available items in DDB but filtering has left us with less
          than the desired page so return the last evaluated DDB key
       b. We've either cycled through all the DDB pages and fulfilled the page size OR we're on the first DDB page still
       c. There's no new events yet so just return whatever the user sent
    2. If the job is finished, return a watermark if the last page hasn't been reached
    """
    next_start = None
    if job_status not in end_statuses:
        # Job is in progress
        if len(items) < page_size and last_evaluated_ddb_key:
            next_start = last_evaluated_ddb_key
        elif 0 < len(items) <= page_size:
            next_start = items[len(items) - 1]["Sk"]
        else:
            next_start = str(initial_start_key)
    # If the job is finished but there are potentially more results
    elif len(items) == page_size and last_query_start_key:
        next_start = items[len(items) - 1]["Sk"]

    return next_start
