"""
Task to purge an SQS queue
"""
import boto3

from decorators import with_logging

sqs = boto3.resource("sqs")


@with_logging
def handler(event, context):
    """
    Purge the queue.

    Args:
        event: (todo): write your description
        context: (dict): write your description
    """
    queue = sqs.Queue(event["QueueUrl"])
    queue.purge()
