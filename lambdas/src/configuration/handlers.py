"""
Configuration handlers
"""
import json
import os

from boto_factory import get_resource


def retrieve_handler(event, context):
    dynamodb_resource = get_resource("dynamodb")
    table = dynamodb_resource.Table(os.getenv("CONFIGURATION_TABLE_NAME"))
    items = table.scan()["Items"]
    return {
        "statusCode": 200,
        "body": json.dumps({"data": items})
    }


def create_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }


def delete_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }
