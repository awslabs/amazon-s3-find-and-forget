import json
import boto3
from crhelper import CfnResource
from decorators import with_logger


helper = CfnResource(json_logging=False, log_level='DEBUG',
                     boto_level='CRITICAL')

settings_file = "settings.js"
s3_client = boto3.client("s3")

@with_logger
@helper.create
@helper.update
def create(event, context):
    props = event.get('ResourceProperties', None)
    bucket = props.get("WebUIBucket")
    with_cloudfront = props.get("CreateCloudFrontDistribution")
    acl = "private" if with_cloudfront == "true" else "public-read"
    settings = {
        "apiUrl": props.get("ApiUrl"),
        "athenaExecutionRole": props.get("AthenaExecutionRole"),
        "cognitoIdentityPool": props.get("CognitoIdentityPoolId"),
        "cognitoUserPoolId": props.get("CognitoUserPoolId"),
        "cognitoUserPoolClientId": props.get("CognitoUserPoolClientId"),
        "deleteTaskRole": props.get("DeleteTaskRole"),
        "region": props.get("Region"),
        "version": props.get("Version")
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
    return None


def handler(event, context):
    helper(event, context)
