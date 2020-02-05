import boto3
from crhelper import CfnResource
from decorators import with_logger


helper = CfnResource(json_logging=False, log_level='DEBUG',
                     boto_level='CRITICAL')

s3 = boto3.resource("s3")


@with_logger
@helper.create
@helper.update
def create(event, context):
    return None


@with_logger
@helper.delete
def delete(event, context):
    props = event['ResourceProperties']
    bucket = s3.Bucket(props["Bucket"])
    bucket.objects.all().delete()
    bucket.object_versions.all().delete()
    return None


def handler(event, context):
    helper(event, context)
