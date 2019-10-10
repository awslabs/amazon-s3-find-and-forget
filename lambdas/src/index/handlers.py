"""
Indexing handlers
"""
import json

def plan_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }


def trigger_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }
