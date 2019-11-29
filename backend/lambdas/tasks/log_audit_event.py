"""
Task to log events to the auditing log stream
"""
from uuid import uuid4

import boto3

from boto_utils import log_event
from decorators import with_logger

client = boto3.client("logs")


@with_logger
def handler(event, context):
    log_stream_prefix = event["JobId"]
    log_stream_suffix = event.get("StreamSuffix", str(uuid4()))
    log_stream = "{}-{}".format(log_stream_prefix, log_stream_suffix)
    event_name = event["EventName"]
    event_data = event["EventData"]
    log_event(client, log_stream, event_name, event_data)
