import boto3
from crhelper import CfnResource
from boto_utils import paginate
from decorators import with_logging


helper = CfnResource(json_logging=False, log_level="DEBUG", boto_level="CRITICAL")

ecr_client = boto3.client("ecr")


@with_logging
@helper.create
@helper.update
def create(event, context):
    """
    Create a new event.

    Args:
        event: (todo): write your description
        context: (str): write your description
    """
    return None


@with_logging
@helper.delete
def delete(event, context):
    """
    Delete image image.

    Args:
        event: (todo): write your description
        context: (dict): write your description
    """
    props = event["ResourceProperties"]
    repository = props["Repository"]
    images = list(
        paginate(
            ecr_client, ecr_client.list_images, ["imageIds"], repositoryName=repository
        )
    )

    if images:
        ecr_client.batch_delete_image(imageIds=images, repositoryName=repository)

    return None


def handler(event, context):
    """
    Emit an event.

    Args:
        event: (todo): write your description
        context: (dict): write your description
    """
    helper(event, context)
