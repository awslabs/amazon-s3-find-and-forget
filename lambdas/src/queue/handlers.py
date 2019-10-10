"""
Queue handlers
"""
import json


def plan_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }


def enqueue_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }


def process_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }
