"""
Configuration handlers
"""
import json
import os

from aws_xray_sdk.core import xray_recorder

from boto_factory import get_resource
from decorators import with_logger


@with_logger
@xray_recorder.capture('RetrieveHandler')
def retrieve_handler(event, context):
    dynamodb_resource = get_resource("dynamodb")
    table = dynamodb_resource.Table(os.getenv("CONFIGURATION_TABLE_NAME"))
    items = table.scan()["Items"]

    return {
        "statusCode": 200,
        "body": json.dumps({"data": items})
    }


@with_logger
def create_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }


@with_logger
def delete_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }
