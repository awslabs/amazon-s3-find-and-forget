from crhelper import CfnResource
from decorators import with_logger

import boto3
import logging
import os

helper = CfnResource(json_logging=False, log_level='DEBUG',
                     boto_level='CRITICAL')

ecs_client = boto3.client("ecs")


@with_logger
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


@with_logger
@helper.delete
def delete(event, context):
    return None


def handler(event, context):
    helper(event, context)
