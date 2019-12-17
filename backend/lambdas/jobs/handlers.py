"""
Job handlers
"""
import json
import os

import boto3
from datetime import datetime, timezone
from aws_xray_sdk.core import xray_recorder

from decorators import with_logger, request_validator, catch_errors, load_schema

sf_client = boto3.client("stepfunctions")
s3 = boto3.resource("s3")
bucket = os.getenv("ResultBucket")


@with_logger
@xray_recorder.capture('GetJobHandler')
@request_validator(load_schema("get_job_handler"), "pathParameters")
@catch_errors
def get_job_handler(event, context):
    job_id = event["pathParameters"]["job_id"]
    resp = sf_client.list_executions(stateMachineArn=os.getenv("StateMachineArn"), statusFilter="RUNNING")
    executions = resp["executions"]
    execution = next((execution for execution in executions if execution["name"] == job_id), None)
    if execution:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "JobId": execution["name"],
                "JobStatus": execution["status"],
                "StartTime": execution["startDate"].isoformat().replace('+00:00', 'Z')
            })
        }
    else:
        summary = get_object_contents(bucket, 'reports/{}/summary.json'.format(job_id))
        return {
            "statusCode": 200,
            "body": summary
        }


def get_object_contents(bucket, key):
    return s3.Object(bucket, key).get()['Body'].read().decode('utf-8')