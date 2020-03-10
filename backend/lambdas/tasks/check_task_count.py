"""
Task to check the number of running and pending tasks
"""
import boto3

from decorators import with_logger

client = boto3.client("ecs")


@with_logger
def handler(event, context):
    try:
        service = client.describe_services(
            cluster=event["Cluster"],
            services=[
                event["ServiceName"],
            ],
        )["services"][0]
        pending = service["pendingCount"]
        running = service["runningCount"]
        return {
            "Pending": pending,
            "Running": running,
            "Total": pending + running
        }
    except IndexError:
        context.logger.error("Unable to find service '%s'", event["ServiceName"])
        raise ValueError("Service {} in cluster {} not found".format(event["ServiceName"], event["Cluster"]))
