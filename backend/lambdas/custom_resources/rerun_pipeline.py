import boto3
from crhelper import CfnResource
from decorators import with_logging


helper = CfnResource(json_logging=False, log_level="DEBUG", boto_level="CRITICAL")

pipe_client = boto3.client("codepipeline")


@with_logging
@helper.delete
def delete(event, context):
    return None


@with_logging
@helper.create
@helper.update
def update(event, context):
    props = event["ResourceProperties"]
    props_old = event.get("OldResourceProperties", {})

    deploy_ui_changed = (
        "DeployWebUI" in props_old
        and props_old["DeployWebUI"] == "false"
        and props["DeployWebUI"] == "true"
    )

    new_version = (
        (not "Version" in props_old or props_old["Version"] != props["Version"])
        if "Version" in props
        else "Version" in props_old
    )

    if deploy_ui_changed or new_version:
        pipe_client.start_pipeline_execution(name=props["PipelineName"])

    return None


def handler(event, context):
    helper(event, context)
