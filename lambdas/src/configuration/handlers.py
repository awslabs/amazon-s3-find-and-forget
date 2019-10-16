"""
Configuration handlers
"""
import json
import os

from aws_xray_sdk.core import xray_recorder

from boto_factory import get_resource
from decorators import with_logger

dynamodb_resource = get_resource("dynamodb")
table = dynamodb_resource.Table(os.getenv("ConfigurationTable"))


@with_logger
@xray_recorder.capture('RetrieveHandler')
def retrieve_handler(event, context):
    items = table.scan()["Items"]

    return {
        "statusCode": 200,
        "body": json.dumps({"data": items})
    }


@with_logger
def create_handler(event, context):
    body = json.loads(event["body"])
    s3_uri = body["S3Uri"]
    columns = body["Columns"]
    s3_trigger = body.get("S3Trigger", True)
    object_types = body.get("ObjectTypes", ["parquet"])
    item = {
        "S3Uri": s3_uri,
        "Columns": columns,
        "S3Trigger": s3_trigger,
        "ObjectTypes": object_types
    }
    table.put_item(Item=item)

    return {
        "statusCode": 200,
        "body": json.dumps(item)
    }


@with_logger
def delete_handler(event, context):
    body = json.loads(event["body"])
    s3_uri = body["S3Uri"]
    table.delete_item(Key={
        "S3Uri": s3_uri
    })

    return {
        "statusCode": 204,
        "body": json.dumps({})
    }
