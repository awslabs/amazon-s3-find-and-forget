"""
Task to check the number of running and pending tasks
"""
import logging
import boto3

from decorators import with_logging

logger = logging.getLogger()
client = boto3.client("ecs")


@with_logging
def handler(event, context):
    """
    Return a single event.

    Args:
        event: (todo): write your description
        context: (dict): write your description
    """
    try:
        service = client.describe_services(
            cluster=event["Cluster"], services=[event["ServiceName"],],
        )["services"][0]
        pending = service["pendingCount"]
        running = service["runningCount"]
        return {"Pending": pending, "Running": running, "Total": pending + running}
    except IndexError:
        logger.error("Unable to find service '%s'", event["ServiceName"])
        raise ValueError(
            "Service {} in cluster {} not found".format(
                event["ServiceName"], event["Cluster"]
            )
        )
