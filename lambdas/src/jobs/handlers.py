"""
Job handlers
"""
import json
import os

from boto_factory import get_client

sf_client = get_client("stepfunctions")


def get_job_handler(event, context):
    job_id = event["pathParameters"]["job_id"]
    execution_arn = get_execution_arn(job_id)
    resp = sf_client.describe_execution(executionArn=execution_arn)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "JobId": resp["name"],
            "Status": resp["status"],
        })
    }


def get_execution_arn(exec_id):
    return "{}:{}".format(os.getenv("StateMachineArn").replace("stateMachine", "execution", 1), exec_id)
