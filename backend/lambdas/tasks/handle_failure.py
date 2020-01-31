"""
Task to handle SF failure events
"""
from boto_utils import emit_event
from decorators import with_logger


@with_logger
def handler(event, context):
    details = event.get("detail")
    job_id = details["name"]
    event_name = "Exception"
    event_data = {
        "Error": "State Machine {}".format(details["status"]),
        "Cause": "Unknown error occurred. Check the execution history for more details"
    }
    emit_event(job_id, event_name, event_data, "CloudWatchEvents")
