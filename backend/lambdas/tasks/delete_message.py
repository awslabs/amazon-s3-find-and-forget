import os

import boto3
from decorators import with_logger


sqs = boto3.resource("sqs")
queue_url = os.getenv("QueueUrl")


@with_logger
def handler(event, context):
    receipt_handle = event.get("ReceiptHandle")
    if receipt_handle:
        message = sqs.Message(queue_url, receipt_handle)
        message.delete()
    else:
        context.logger.warn("No receipt handle found in event. Skipping")
