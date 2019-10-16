"""
Queue handlers
"""
import json


def get_job_handler(event, context):
    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }
