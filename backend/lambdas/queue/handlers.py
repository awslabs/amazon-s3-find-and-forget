"""
Queue handlers
"""

import random
import json
import os
import uuid

import boto3

from decimal import Decimal

from boto_utils import (
    DecimalEncoder,
    get_config,
    get_user_info,
    paginate,
    running_job_exists,
    utc_timestamp,
    deserialize_item,
)
from decorators import (
    with_logging,
    catch_errors,
    add_cors_headers,
    json_body_loader,
    load_schema,
    request_validator,
)

sfn_client = boto3.client("stepfunctions")
ddb_client = boto3.client("dynamodb")
ddb_resource = boto3.resource("dynamodb")

deletion_queue_table_name = os.getenv("DeletionQueueTable", "S3F2_DeletionQueue")
deletion_queue_table = ddb_resource.Table(deletion_queue_table_name)
jobs_table = ddb_resource.Table(os.getenv("JobTable", "S3F2_Jobs"))
bucket_count = int(os.getenv("GSIBucketCount", 1))
max_size_bytes = 375000


@with_logging
@add_cors_headers
@json_body_loader
@catch_errors
def enqueue_handler(event, context):
    body = event["body"]
    validate_queue_items([body])
    user_info = get_user_info(event)
    item = enqueue_items([body], user_info)[0]
    return {"statusCode": 201, "body": json.dumps(item, cls=DecimalEncoder)}


@with_logging
@add_cors_headers
@json_body_loader
@catch_errors
def enqueue_batch_handler(event, context):
    body = event["body"]
    matches = body["Matches"]
    validate_queue_items(matches)
    user_info = get_user_info(event)
    items = enqueue_items(matches, user_info)
    return {
        "statusCode": 201,
        "body": json.dumps({"Matches": items}, cls=DecimalEncoder),
    }


@with_logging
@add_cors_headers
@request_validator(load_schema("list_queue_items"))
@catch_errors
def get_handler(event, context):
    defaults = {"Type": "Simple"}
    qs = event.get("queryStringParameters")
    if not qs:
        qs = {}
    page_size = int(qs.get("page_size", 10))
    scan_params = {"Limit": page_size}
    start_at = qs.get("start_at")
    if start_at:
        scan_params["ExclusiveStartKey"] = {"DeletionQueueItemId": start_at}
    items = deletion_queue_table.scan(**scan_params).get("Items", [])
    if len(items) < page_size:
        next_start = None
    else:
        next_start = items[-1]["DeletionQueueItemId"]
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "MatchIds": list(map(lambda item: dict(defaults, **item), items)),
                "NextStart": next_start,
            },
            cls=DecimalEncoder,
        ),
        "headers": {"Access-Control-Expose-Headers": "content-length"},
    }


@with_logging
@add_cors_headers
@json_body_loader
@catch_errors
def cancel_handler(event, context):
    if running_job_exists():
        raise ValueError("Cannot delete matches whilst there is a job in progress")
    body = event["body"]
    matches = body["Matches"]
    with deletion_queue_table.batch_writer() as batch:
        for match in matches:
            batch.delete_item(Key={"DeletionQueueItemId": match["DeletionQueueItemId"]})

    return {"statusCode": 204}


@with_logging
@add_cors_headers
@catch_errors
def process_handler(event, context):
    if running_job_exists():
        raise ValueError("There is already a job in progress")

    job_id = str(uuid.uuid4())
    config = get_config()
    item = {
        "Id": job_id,
        "Sk": job_id,
        "Type": "Job",
        "JobStatus": "QUEUED",
        "GSIBucket": str(random.randint(0, bucket_count - 1)),
        "CreatedAt": utc_timestamp(),
        "CreatedBy": get_user_info(event),
        **{k: v for k, v in config.items() if k not in ["JobDetailsRetentionDays"]},
    }
    if int(config.get("JobDetailsRetentionDays", 0)) > 0:
        item["Expires"] = utc_timestamp(days=config["JobDetailsRetentionDays"])
    jobs_table.put_item(Item=item)
    return {"statusCode": 202, "body": json.dumps(item, cls=DecimalEncoder)}


def validate_queue_items(items):
    for item in items:
        if item.get("Type", "Simple") == "Composite":
            is_array = isinstance(item["MatchId"], list)
            enough_columns = is_array and len(item["MatchId"]) > 0
            just_one_mapper = len(item["DataMappers"]) == 1
            if not is_array:
                raise ValueError(
                    "MatchIds of Composite type need to be specified as array"
                )
            if not enough_columns:
                raise ValueError(
                    "MatchIds of Composite type need to have a value for at least one column"
                )
            if not just_one_mapper:
                raise ValueError(
                    "MatchIds of Composite type need to be associated to exactly one Data Mapper"
                )


def enqueue_items(matches, user_info):
    items = []
    with deletion_queue_table.batch_writer() as batch:
        for match in matches:
            match_id = match["MatchId"]
            data_mappers = match.get("DataMappers", [])
            item = {
                "DeletionQueueItemId": str(uuid.uuid4()),
                "Type": match.get("Type", "Simple"),
                "MatchId": match_id,
                "CreatedAt": utc_timestamp(),
                "DataMappers": data_mappers,
                "CreatedBy": user_info,
            }
            batch.put_item(Item=item)
            items.append(item)
    return items
