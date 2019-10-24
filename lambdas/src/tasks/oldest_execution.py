"""
Task to check if the current execution is the oldest
"""
import os
from collections import deque

import boto3

from boto_utils import paginate
from decorators import with_logger

sf_client = boto3.client("stepfunctions")


@with_logger
def handler(event, context):
    executions = paginate(sf_client, sf_client.list_executions, "executions", **{
        "stateMachineArn": os.getenv("StateMachineArn"),
        "statusFilter": "RUNNING"
    })
    dd = deque(executions, maxlen=1)
    oldest = dd.pop()
    context.logger.info("Oldest execution: {}".format(oldest["executionArn"]))

    return event["ExecutionId"] == oldest["executionArn"]
