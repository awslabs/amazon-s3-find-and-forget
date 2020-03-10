"""
Task to check the SQS Queue Size
"""
import boto3

from decorators import with_logging

sqs = boto3.resource('sqs')


def get_attribute(q, attribute):
    return int(q.attributes[attribute])


@with_logging
def handler(event, context):
    queue = sqs.Queue(event["QueueUrl"])
    visible = get_attribute(queue, "ApproximateNumberOfMessages")
    not_visible = get_attribute(queue, "ApproximateNumberOfMessagesNotVisible")
    return {
        "Visible": visible,
        "NotVisible": not_visible,
        "Total": visible + not_visible
    }
