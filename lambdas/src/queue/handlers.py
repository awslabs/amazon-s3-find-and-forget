"""
Queue handlers
"""
import json


def enqueue_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }


def get_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }


def cancel_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }


def process_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }
