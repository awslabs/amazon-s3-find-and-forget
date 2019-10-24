"""
Task to check if the current execution is the oldest
"""
import os
from collections import deque

from boto_factory import get_client

client = get_client("stepfunctions")


def paginate(client, method, **kwargs):
    paginator = client.get_paginator(method.__name__)
    for page in paginator.paginate(**kwargs).result_key_iters():
        for result in page:
            yield result


def handler(event, context):
    executions = paginate(client, client.list_exectuions, **{
        "stateMachineArn": os.getenv("StateMachineArn"),
        "statusFilter": "RUNNING"
    })
    dd = deque(executions, maxlen=1)
    oldest = dd.pop()

    return event["ExecutionId"] == oldest
