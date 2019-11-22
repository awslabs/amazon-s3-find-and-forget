"""
Task to check the SQS Queue Size
"""
import boto3

from decorators import with_logger

sqs = boto3.resource('sqs')


def get_attribute(q, attribute):
    return int(q.attributes[attribute])


@with_logger
def handler(event, context):
    queue = sqs.Queue(event["QueueUrl"])
    return (get_attribute(queue, "ApproximateNumberOfMessages") +
            get_attribute(queue, "ApproximateNumberOfMessagesNotVisible"))
