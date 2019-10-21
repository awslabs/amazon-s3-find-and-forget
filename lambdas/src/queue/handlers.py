"""
Queue handlers
"""
import json
import os

from aws_xray_sdk.core import xray_recorder

from boto_factory import get_resource, get_client
from decorators import with_logger


sfn_client = get_client("stepfunctions")
dynamodb_resource = get_resource("dynamodb")
table = dynamodb_resource.Table(os.getenv("DeletionQueueTable"))


@with_logger
@xray_recorder.capture('EnqueueHandler')
def enqueue_handler(event, context):
    body = json.loads(event["body"])
    match_id = body["MatchId"]
    columns = body.get("Columns", [])
    item = {
        "MatchId": match_id,
        "Columns": columns,
    }
    table.put_item(Item=item)

    return {
        "statusCode": 201,
        "body": json.dumps(item)
    }


@with_logger
@xray_recorder.capture('GetQueueHandler')
def get_handler(event, context):
    items = table.scan()["Items"]

    return {
        "statusCode": 200,
        "body": json.dumps({"MatchIds": items})
    }


@with_logger
@xray_recorder.capture('CancelDeletionHandler')
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
