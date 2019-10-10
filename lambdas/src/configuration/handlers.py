"""
Configuration handlers
"""
import json


def retrieve_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
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
