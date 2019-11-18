"""
Task to scan a DynamoDB table
"""
import boto3
from boto3.dynamodb.types import TypeDeserializer

from decorators import with_logger
from boto_utils import paginate

ddb_client = boto3.client('dynamodb')
deserializer = TypeDeserializer()


@with_logger
def handler(event, context):
    results = paginate(ddb_client, ddb_client.scan, "Items", **{
        "TableName": event["TableName"]
    })

    items = [deserialize_item(result) for result in results]

    return {
        "Items": items,
        "Count": len(items)
    }


def deserialize_item(item):
    return {
        k: deserializer.deserialize(v) for k, v in item.items()
    }
