import boto3
from crhelper import CfnResource
from decorators import with_logging


helper = CfnResource(json_logging=False, log_level="DEBUG", boto_level="CRITICAL")

pipe_client = boto3.client("codepipeline")


@with_logging
@helper.create
@helper.delete
def create(event, context):
    return None


@with_logging
@helper.update
def update(event, context):
    props = event["ResourceProperties"]
    pipe_client.start_pipeline_execution(name=props["PipelineName"])
    return None


def handler(event, context):
    helper(event, context)
