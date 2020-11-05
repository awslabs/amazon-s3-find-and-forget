import os
import boto3
from crhelper import CfnResource
from decorators import with_logging

helper = CfnResource(json_logging=False, log_level="DEBUG", boto_level="CRITICAL")

ecs_client = boto3.client("ecs")


@with_logging
@helper.create
@helper.update
def create(event, context):
    """
    Create an instance

    Args:
        event: (todo): write your description
        context: (str): write your description
    """
    ecs_client.update_cluster_settings(
        cluster=os.getenv("Cluster"),
        settings=[{"name": "containerInsights", "value": "enabled"}],
    )
    return None


@with_logging
@helper.delete
def delete(event, context):
    """
    Deletes an event.

    Args:
        event: (todo): write your description
        context: (dict): write your description
    """
    return None


def handler(event, context):
    """
    Emit an event.

    Args:
        event: (todo): write your description
        context: (dict): write your description
    """
    helper(event, context)
