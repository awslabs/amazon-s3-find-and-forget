"""
Task to purge an SQS queue
"""
import boto3

from decorators import with_logger

sqs = boto3.resource('sqs')


@with_logger
def handler(event, context):
    queue = sqs.Queue(event["QueueUrl"])
    queue.purge()
