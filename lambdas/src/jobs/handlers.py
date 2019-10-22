"""
Job handlers
"""
import json
import os

from aws_xray_sdk.core import xray_recorder

from boto_factory import get_client
from decorators import with_logger, request_validator, catch_errors, load_schema

sf_client = get_client("stepfunctions")


@with_logger
@xray_recorder.capture('GetJobHandler')
@request_validator(load_schema("get_job_handler"), "pathParameters")
@catch_errors
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
