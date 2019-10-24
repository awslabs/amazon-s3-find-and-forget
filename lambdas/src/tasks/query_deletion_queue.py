"""
Task to scan the DynamoDB deletion queue table
"""
import os

import boto3

from decorators import with_logger
from boto_utils import paginate

ddb_client = boto3.client('dynamodb')


@with_logger
def handler(event, context):
    results = paginate(ddb_client, ddb_client.scan, "Items", **{
      "TableName": os.getenv("DeletionQueueTableName")
    })

    return [
        r for r in results
    ]
