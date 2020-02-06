"""
Task to scan a DynamoDB table
"""
import boto3

from decorators import with_logger
from boto_utils import paginate

ddb_client = boto3.client('dynamodb')


@with_logger
def handler(event, context):
    results = list(paginate(ddb_client, ddb_client.scan, "Items", **{
        "TableName": event["TableName"]
    }))

    return {
        "Items": results,
        "Count": len(results)
    }