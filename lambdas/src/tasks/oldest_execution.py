"""
Task to check if the current execution is the oldest
"""
import os
from collections import deque

import boto3

from decorators import with_logger

sf_client = boto3.client("stepfunctions")


def paginate(client, method, **kwargs):
    paginator = client.get_paginator(method.__name__)
    for page in paginator.paginate(**kwargs).result_key_iters():
        for result in page:
            yield result


@with_logger
def handler(event, context):
    executions = paginate(sf_client, sf_client.list_executions, **{
        "stateMachineArn": os.getenv("StateMachineArn"),
        "statusFilter": "RUNNING"
    })
    dd = deque(executions, maxlen=1)
    oldest = dd.pop()
    context.logger.info("Oldest execution: {}".format(oldest["executionArn"]))

    return event["ExecutionId"] == oldest["executionArn"]
