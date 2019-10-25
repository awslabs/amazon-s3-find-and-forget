"""
Task to clear the query results
"""
import os

import boto3

from decorators import with_logger

s3 = boto3.resource('s3')
bucket = s3.Bucket(os.getenv("BucketName"))


@with_logger
def handler(event, context):
    # Assume results will be stored under the Execution ID
    responses = bucket.objects.filter(Prefix="{}/".format(event["ExecutionId"])).delete()
    deleted = [item for response in responses for item in response.get("Deleted", [])]
    errors = [item for response in responses for item in response.get("Errors", [])]
    if len(errors) > 0:
        context.logger.error("Responses from S3 contain errors: %s", responses)
        raise RuntimeError("Failed to delete some objects: %s", errors)

    return deleted
