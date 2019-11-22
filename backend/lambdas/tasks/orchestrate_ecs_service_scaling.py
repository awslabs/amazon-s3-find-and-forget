"""
Task to orchestrate scaling for a ECS Service
"""
import boto3

from decorators import with_logger

ecs = boto3.client('ecs')


@with_logger
def handler(event, context):
    cluster = event["Cluster"]
    max_tasks = event["DeletionTasksMaxNumber"]
    queue_size = event["QueueSize"]
    service = event["DeleteService"]
    desired_count = min(queue_size, max_tasks)
    ecs.update_service(
        cluster=cluster,
        service=service,
        desiredCount=desired_count
    )

    return desired_count
