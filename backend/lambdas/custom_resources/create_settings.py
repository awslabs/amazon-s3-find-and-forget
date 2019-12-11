from crhelper import CfnResource
from decorators import with_logger

import boto3
import json
import logging
import os

helper = CfnResource(json_logging=False, log_level='DEBUG',
                     boto_level='CRITICAL')

bucket = os.getenv("WebUIBucket")
settings_file = "settings.js"
s3_client = boto3.client("s3")

@with_logger
@helper.create
@helper.update
def create(event, context):
    with_cloudfront = os.getenv("CreateCloudFrontDistribution")
    acl = "private" if with_cloudfront == "true" else "public-read"
    settings = {
        "apiUrl": os.getenv("ApiUrl"),
        "cognitoIdentityPool": os.getenv("CognitoIdentityPoolId"),
        "cognitoUserPoolId": os.getenv("CognitoUserPoolId"),
        "cognitoUserPoolClientId": os.getenv("CognitoUserPoolClientId"),
        "region": os.getenv("Region")
    }
    s3_client.put_object(
        ACL=acl,
        Bucket=bucket,
        Key=settings_file,
        Body="window.s3f2Settings={}".format(json.dumps(settings))
    )
    return "arn:aws:s3:::{}/{}".format(bucket, settings_file)


@with_logger
@helper.delete
def delete(event, context):
    s3_client.delete_object(
        Bucket=bucket,
        Key=settings_file
    )
    return None


def handler(event, context):
    helper(event, context)
