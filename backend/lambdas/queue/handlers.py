"""
Queue handlers
"""
import logging
from datetime import datetime, timezone
import random
import json
import os
import uuid

import boto3
from botocore.exceptions import ClientError

from boto_utils import DecimalEncoder, running_job_exists
from decorators import with_logger, request_validator, catch_errors, load_schema, add_cors_headers

logger = logging.getLogger()
logger.setLevel(logging.INFO)
sfn_client = boto3.client("stepfunctions")
ssm = boto3.client('ssm')
dynamodb_resource = boto3.resource("dynamodb")
deletion_queue_table = dynamodb_resource.Table(os.getenv("DeletionQueueTable", "S3F2_DeletionQueue"))
jobs_table = dynamodb_resource.Table(os.getenv("JobTable", "S3F2_Jobs"))
index = os.getenv("JobTableDateGSI", "Date-GSI")
bucket_count = int(os.getenv("GSIBucketCount", 1))


@with_logger
@add_cors_headers
@request_validator(load_schema("queue_item"), "body")
@catch_errors
def enqueue_handler(event, context):
    body = json.loads(event["body"])
    match_id = body["MatchId"]
    data_mappers = body.get("DataMappers", [])
    item = {
        "MatchId": match_id,
        "CreatedAt": round(datetime.now(timezone.utc).timestamp()),
        "DataMappers": data_mappers,
    }
    deletion_queue_table.put_item(Item=item)

    return {
        "statusCode": 201,
        "body": json.dumps(item, cls=DecimalEncoder)
    }


@with_logger
@add_cors_headers
@catch_errors
def get_handler(event, context):
    items = deletion_queue_table.scan()["Items"]

    return {
        "statusCode": 200,
        "body": json.dumps({"MatchIds": items}, cls=DecimalEncoder)
    }


@with_logger
@add_cors_headers
@request_validator(load_schema("cancel_handler"), "body")
@catch_errors
def cancel_handler(event, context):
    if running_job_exists():
        raise ValueError("Cannot delete matches whilst there is a job in progress")
    body = json.loads(event["body"])
    matches = body["Matches"]
    with deletion_queue_table.batch_writer() as batch:
        for match in matches:
            batch.delete_item(Key={
                "MatchId": match["MatchId"],
                "CreatedAt": match["CreatedAt"],
            })

    return {
        "statusCode": 204
    }


@with_logger
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
        "CreatedAt": round(datetime.now(timezone.utc).timestamp()),
        "Matches": deletion_queue_table.scan()["Items"],
        **config,
    }

    jobs_table.put_item(Item=item)

    return {
        "statusCode": 202,
        "body": json.dumps(item, cls=DecimalEncoder)
    }


def get_config():
    try:
        param_name = os.getenv("ConfigParam", "S3F2-Configuration")
        return json.loads(ssm.get_parameter(Name=param_name, WithDecryption=True)["Parameter"]["Value"])
    except (KeyError, ValueError) as e:
        logger.error("Invalid configuration supplied: {}".format(str(e)))
        raise e
    except ClientError as e:
        logger.error("Unable to retrieve config: {}".format(str(e)))
        raise e
    except Exception as e:
        logger.error("Unknown error retrieving config: {}".format(str(e)))
        raise e
