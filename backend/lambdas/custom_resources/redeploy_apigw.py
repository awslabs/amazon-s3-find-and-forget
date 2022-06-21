import boto3
from crhelper import CfnResource
from decorators import with_logging


helper = CfnResource(json_logging=False, log_level="DEBUG", boto_level="CRITICAL")

api_client = boto3.client("apigateway")


@with_logging
@helper.create
@helper.delete
def create(event, context):
    return None


@with_logging
@helper.update
def update(event, context):
    props = event["ResourceProperties"]
    props_old = event["OldResourceProperties"]
    if props_old["DeployCognito"] != props["DeployCognito"]:
        api_client.create_deployment(
            restApiId=props["ApiId"], stageName=props["ApiStage"]
        )
    return None


def handler(event, context):
    helper(event, context)
