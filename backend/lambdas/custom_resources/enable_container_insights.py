import os
import boto3
from crhelper import CfnResource
from decorators import with_logging

helper = CfnResource(json_logging=False, log_level='DEBUG',
                     boto_level='CRITICAL')

ecs_client = boto3.client("ecs")


@with_logging
@helper.create
@helper.update
def create(event, context):
    ecs_client.update_cluster_settings(
        cluster=os.getenv("Cluster"),
        settings=[
            {
                'name': 'containerInsights',
                'value': 'enabled'
            }
        ]
    )
    return None


@with_logging
@helper.delete
def delete(event, context):
    return None


def handler(event, context):
    helper(event, context)
