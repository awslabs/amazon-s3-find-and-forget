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

s3 = boto3.resource("s3")
sfn_client = boto3.client("stepfunctions")
ddb_client = boto3.client("dynamodb")
ddb_resource = boto3.resource("dynamodb")

deletion_queue_table_name = os.getenv("DeletionQueueTable", "S3F2_DeletionQueue")
deletion_queue_table = ddb_resource.Table(deletion_queue_table_name)
jobs_table = ddb_resource.Table(os.getenv("JobTable", "S3F2_Jobs"))
bucket_count = int(os.getenv("GSIBucketCount", 1))
max_size_bytes = 375000
deletion_queue_bucket = os.getenv("JobBucket")

data_mapper_table_name = os.getenv("DataMapperTable")


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
    data_mappers_table = get_data_mappers()
    relevant_mappers = [da for da in data_mappers_table if da["DataMapperId"] in data_mappers] if len(data_mappers) > 0 else data_mappers_table
    number_of_match_ids = match_id.count(",") + 1
    if all(len(r["Columns"]) == number_of_match_ids for r in relevant_mappers):
        deletion_queue_table.put_item(Item=item)
        return {
            "statusCode": 201,
            "body": json.dumps(item, cls=DecimalEncoder)
        }
    else:
        raise ValueError("Cannot add DeletionQueueItem with data mappers that has different number of match columns")


def get_data_mappers():
    results = paginate(ddb_client, ddb_client.scan, "Items", TableName=data_mapper_table_name)
    for result in results:
        yield deserialize_item(result)


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

    neura_env = event.get("headers", {}).get("neura_env", "staging")
    job_id = str(uuid.uuid4())
    config = get_config()
    deletion_queue_key = 'jobs/{}/deletion_queue/data.json'.format(job_id)
    item = {
        "Id": job_id,
        "Sk": job_id,
        "Type": "Job",
        "JobStatus": "QUEUED",
        "GSIBucket": str(random.randint(0, bucket_count - 1)),
        "CreatedAt": utc_timestamp(),
        "DeletionQueueBucket": deletion_queue_bucket,
        "DeletionQueueKey": deletion_queue_key,
        "DeletionQueueItemsSkipped": False,
        "NeuraEnv": neura_env,
        "CreatedBy": get_user_info(event),
        **{k: v for k, v in config.items() if k not in ["JobDetailsRetentionDays"]}
    }

    if int(config.get("JobDetailsRetentionDays", 0)) > 0:
        item["Expires"] = utc_timestamp(days=config["JobDetailsRetentionDays"])

    item_size_bytes = calculate_ddb_item_bytes(item)
    deletion_queue_items = {
     "DeletionQueueItems": []
    }
    first = True
    prev_num_of_cols = 1
    for extended_deletion_queue_item in get_deletion_queue():
        deletion_item = {
            "DeletionQueueItemId": extended_deletion_queue_item["DeletionQueueItemId"],
            "MatchId": extended_deletion_queue_item["MatchId"],
            "DataMappers": extended_deletion_queue_item["DataMappers"]
        }
        deletion_queue_items["DeletionQueueItems"].append(deletion_item)
        number_of_cols = extended_deletion_queue_item["MatchId"].count(",") + 1
        if not first and number_of_cols != prev_num_of_cols:
            raise ValueError("Cannot start job with different number of columns")
        first = False
        prev_num_of_cols = number_of_cols
        # current_size_bytes = calculate_ddb_item_bytes(deletion_queue_item)
        # if item_size_bytes + current_size_bytes < max_size_bytes:
        #     item['DeletionQueueItems'].append(deletion_queue_item)
        #     item_size_bytes += current_size_bytes
        # else:
        #     item['DeletionQueueItemsSkipped'] = True
        #     break
    obj = s3.Object(deletion_queue_bucket, deletion_queue_key)
    obj.put(Body=json.dumps(deletion_queue_items))
    jobs_table.put_item(Item=item)

    # after sending the data to dynamo add the deletion_queue to the response
    item["DeletionQueueItems"] = list(map(lambda x: x["MatchId"], deletion_queue_items["DeletionQueueItems"]))

    # add to athena deletion_queue
    obj = s3.Object(deletion_queue_bucket, 'deletion_keys/deletion_queue/data.csv')
    obj.put(Body="user_id\n" + '\n'.join(item["DeletionQueueItems"]))

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
