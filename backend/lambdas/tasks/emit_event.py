"""
Task to emit events
"""
from uuid import uuid4

from boto_utils import emit_event
from decorators import with_logging


@with_logging
def handler(event, context):
    job_id = event["JobId"]
    event_name = event["EventName"]
    event_data = event["EventData"]
    emitter_id = event.get("EmitterId", str(uuid4()))
    emit_event(job_id, event_name, event_data, emitter_id)
