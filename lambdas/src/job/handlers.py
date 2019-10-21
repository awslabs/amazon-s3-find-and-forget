"""
Queue handlers
"""
import json
import os

from boto_factory import get_client


def get_job_handler(event, context):
    job_id = event["pathParameters"]["job_id"]
    execution_arn = "{}:{}".format(os.getenv("StateMachineArn").replace("stateMachine", "execution"), job_id)
    sf_client = get_client("stepfunctions")
    resp = sf_client.describe_execution()

    return {
        "statusCode": 200,
        "body": json.dumps({"hello": "world"})
    }
