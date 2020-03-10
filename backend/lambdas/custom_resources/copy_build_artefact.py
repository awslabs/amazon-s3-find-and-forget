import boto3
from crhelper import CfnResource
from decorators import with_logging


helper = CfnResource(json_logging=False, log_level='DEBUG',
                     boto_level='CRITICAL')

s3_client = boto3.client("s3")

@with_logging
@helper.create
@helper.update
def create(event, context):
    props = event.get('ResourceProperties', None)
    version = props.get("Version")
    destination_artefact = props.get("ArtefactName")
    destination_bucket = props.get("CodeBuildArtefactBucket")
    source_bucket = props.get("PreBuiltArtefactsBucket")
    source_artefact = "{}/amazon-s3-find-and-forget/{}/build.zip".format(source_bucket, version)

    s3_client.copy_object(
        Bucket=destination_bucket,
        CopySource=source_artefact,
        Key=destination_artefact
    )

    return "arn:aws:s3:::{}/{}".format(destination_bucket, destination_artefact)


@with_logging
@helper.delete
def delete(event, context):
    return None


def handler(event, context):
    helper(event, context)
