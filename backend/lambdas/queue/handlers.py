"""
Queue handlers
"""
import random
import json
import os
import uuid

import boto3

from decimal import Decimal

from boto_utils import DecimalEncoder, get_config, get_user_info, paginate, running_job_exists, \
    utc_timestamp, deserialize_item
from decorators import with_logging, catch_errors, add_cors_headers, json_body_loader, \
    load_schema, request_validator

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
    match_id = body["MatchId"]
    data_mappers = body.get("DataMappers", [])
    item = {
        "DeletionQueueItemId": str(uuid.uuid4()),
        "MatchId": match_id,
        "CreatedAt": utc_timestamp(),
        "DataMappers": data_mappers,
        "CreatedBy": get_user_info(event)
    }
    deletion_queue_table.put_item(Item=item)

    return {
        "statusCode": 201,
        "body": json.dumps(item, cls=DecimalEncoder)
    }


@with_logging
@add_cors_headers
@request_validator(load_schema("list_queue_items"))
@catch_errors
def get_handler(event, context):
    qs = event.get("queryStringParameters")
    if not qs:
        qs = {}
    page_size = int(qs.get("page_size", 10))
    scan_params = {'Limit': page_size}
    start_at = qs.get("start_at")
    if start_at:
        scan_params['ExclusiveStartKey'] = {
            'DeletionQueueItemId': start_at
        }
    items = deletion_queue_table.scan(**scan_params).get("Items", [])
    if len(items) < page_size:
        next_start = None
    else:
        next_start = items[-1]['DeletionQueueItemId']
    return {
        "statusCode": 200,
        "body": json.dumps({
            "MatchIds": items,
            "NextStart": next_start
        }, cls=DecimalEncoder)
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
            batch.delete_item(Key={
                "DeletionQueueItemId": match["DeletionQueueItemId"]
            })

    return {
        "statusCode": 204
    }


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
        "DeletionQueueItems": [],
        "DeletionQueueItemsSkipped": False,
        "CreatedBy": get_user_info(event),
        **{k: v for k, v in config.items() if k not in ["JobDetailsRetentionDays"]}
    }

    if int(config.get("JobDetailsRetentionDays", 0)) > 0:
        item["Expires"] = utc_timestamp(days=config["JobDetailsRetentionDays"])
    
    item_size_bytes = calculate_ddb_item_bytes(item)

    for deletion_queue_item in get_deletion_queue():
        current_size_bytes = calculate_ddb_item_bytes(deletion_queue_item)
        if item_size_bytes + current_size_bytes < max_size_bytes:
            item['DeletionQueueItems'].append(deletion_queue_item)
            item_size_bytes += current_size_bytes
        else:
            item['DeletionQueueItemsSkipped'] = True
            break

    jobs_table.put_item(Item=item)

    return {
        "statusCode": 202,
        "body": json.dumps(item, cls=DecimalEncoder)
    }


def get_deletion_queue():
    results = paginate(ddb_client, ddb_client.scan, "Items", TableName=deletion_queue_table_name)
    for result in results:
        yield deserialize_item(result)


def calculate_ddb_item_bytes(item):
    """
    Basic DynamoDB item size calculator, based on 
    https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
    Note: only relevant types are supported here (numbers, null, bool, string, list, map)
    """
    size = 0
    if item == None: return size
    for key in item:
        size += len(key.encode('utf-8'))
        size += calculate_attribute_size_bytes(item[key])
    return size


def calculate_attribute_size_bytes(attr):
    attr_size = 0
    if attr == None or isinstance(attr, bool):
        attr_size += 1
    elif isinstance(attr, str):
        attr_size += len(attr.encode('utf-8'))
    elif isinstance(attr, (int, float, Decimal)):
        # the max value is used here as the official docs indicate
        # that the calculation for numbers is "approximate"
        attr_size += 21 
    elif isinstance(attr, list):
        attr_size += 3
        for item in attr:
            attr_size += calculate_attribute_size_bytes(item)
    elif isinstance(attr, dict):
        attr_size += 3
        attr_size += calculate_ddb_item_bytes(attr)

    return attr_size
