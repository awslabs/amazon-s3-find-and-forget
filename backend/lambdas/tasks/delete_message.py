import os

import logging
import boto3
from decorators import with_logging

logger = logging.getLogger()
sqs = boto3.resource("sqs")
queue_url = os.getenv("QueueUrl")


@with_logging
def handler(event, context):
    receipt_handle = event.get("ReceiptHandle")
    if receipt_handle:
        message = sqs.Message(queue_url, receipt_handle)
        message.delete()
    else:
        logger.warning("No receipt handle found in event. Skipping")
