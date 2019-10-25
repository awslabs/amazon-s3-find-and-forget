"""
Queue handlers
"""
import json
import os

import boto3
from aws_xray_sdk.core import xray_recorder

from decorators import with_logger, request_validator, catch_errors, load_schema

sfn_client = boto3.client("stepfunctions")
dynamodb_resource = boto3.resource("dynamodb")
table = dynamodb_resource.Table(os.getenv("DeletionQueueTable"))


@with_logger
@xray_recorder.capture('EnqueueHandler')
@request_validator(load_schema("queue_item"), "body")
@catch_errors
def enqueue_handler(event, context):
    body = json.loads(event["body"])
    match_id = body["MatchId"]
    configurations = body.get("Configurations", [])
    item = {
        "MatchId": match_id,
        "Configurations": configurations,
    }
    table.put_item(Item=item)

    return {
        "statusCode": 201,
        "body": json.dumps(item)
    }


@with_logger
@xray_recorder.capture('GetQueueHandler')
@catch_errors
def get_handler(event, context):
    items = table.scan()["Items"]

    return {
        "statusCode": 200,
        "body": json.dumps({"MatchIds": items})
    }


@with_logger
@xray_recorder.capture('CancelDeletionHandler')
@request_validator(load_schema("cancel_handler"), "pathParameters")
@catch_errors
def cancel_handler(event, context):
    match_id = event["pathParameters"]["match_id"]
    table.delete_item(Key={
        "MatchId": match_id
    })

    return {
        "statusCode": 204,
    }


@with_logger
@xray_recorder.capture('ProcessDeletionHandler')
@catch_errors
def process_handler(event, context):
    response = sfn_client.start_execution(
        stateMachineArn=os.getenv("StateMachineArn")
    )
    return {
        "statusCode": 202,
        "body": json.dumps({
            "JobId": response["executionArn"].rsplit(":", 1)[-1]
        })
    }
