import boto3
from crhelper import CfnResource
from boto_utils import convert_iso8601_to_epoch
from decorators import with_logging


helper = CfnResource(json_logging=False, log_level="DEBUG", boto_level="CRITICAL")

ecr_client = boto3.client("ecr")
s3_client = boto3.resource("s3")


@with_logging
@helper.create
@helper.update
@helper.delete
def create(event, context):
    """
    Create a new event.

    Args:
        event: (todo): write your description
        context: (str): write your description
    """
    return None


@with_logging
@helper.poll_create
@helper.poll_update
def poll(event, context):
    """
    Poll image image.

    Args:
        event: (dict): write your description
        context: (dict): write your description
    """
    props = event.get("ResourceProperties", None)
    bucket = props.get("CodeBuildArtefactBucket")
    key = props.get("ArtefactName")
    repository = props.get("ECRRepository")
    obj = s3_client.Object(bucket, key)
    last_modified = convert_iso8601_to_epoch(str(obj.last_modified))
    image_pushed_at = get_latest_image_push(repository)
    return image_pushed_at and last_modified < image_pushed_at


def handler(event, context):
    """
    Emit an event.

    Args:
        event: (todo): write your description
        context: (dict): write your description
    """
    helper(event, context)


def get_latest_image_push(repository):
    """
    Gets the image repository that the repository is enabled.

    Args:
        repository: (str): write your description
    """
    try:
        images = ecr_client.describe_images(
            repositoryName=repository, imageIds=[{"imageTag": "latest"}]
        )

        return convert_iso8601_to_epoch(str(images["imageDetails"][0]["imagePushedAt"]))
    except ecr_client.exceptions.ImageNotFoundException:
        return None
