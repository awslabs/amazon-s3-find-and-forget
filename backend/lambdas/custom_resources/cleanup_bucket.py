import boto3
from crhelper import CfnResource
from decorators import with_logging


helper = CfnResource(json_logging=False, log_level="DEBUG", boto_level="CRITICAL")

s3 = boto3.resource("s3")


@with_logging
def empty_bucket(bucket_name):
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()
    bucket.object_versions.all().delete()


@with_logging
@helper.create
def create(event, context):
    return None


@with_logging
@helper.update
def update(event, context):
    props = event["ResourceProperties"]
    props_old = event["OldResourceProperties"]
    web_ui_deployed = props_old.get("DeployWebUI", "true")
    if web_ui_deployed == "true" and props["DeployWebUI"] == "false":
        empty_bucket(props["Bucket"])
    return None


@with_logging
@helper.delete
def delete(event, context):
    props = event["ResourceProperties"]
    empty_bucket(props["Bucket"])
    return None


def handler(event, context):
    helper(event, context)
