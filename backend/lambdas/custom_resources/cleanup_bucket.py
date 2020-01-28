from crhelper import CfnResource
from decorators import with_logger

import boto3
import json
import logging

helper = CfnResource(json_logging=False, log_level='DEBUG',
                     boto_level='CRITICAL')

s3_client = boto3.client("s3")


@with_logger
@helper.create
@helper.update
def create(event, context):
    return None


@with_logger
@helper.delete
def delete(event, context):
    props = event.get('ResourceProperties', None)
    bucket = props.get("Bucket")
    objects = s3_client.list_objects_v2(Bucket=bucket)
    for obj in objects.get("Contents"):
        s3_client.delete_object(
            Bucket=bucket,
            Key=obj.get("Key")
        )
    return None


def handler(event, context):
    helper(event, context)
