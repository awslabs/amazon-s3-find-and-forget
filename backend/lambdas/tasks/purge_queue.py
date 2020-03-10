"""
Task to purge an SQS queue
"""
import boto3

from decorators import with_logging

sqs = boto3.resource('sqs')


@with_logging
def handler(event, context):
    queue = sqs.Queue(event["QueueUrl"])
    queue.purge()
