import json
import os
import logging
from collections.abc import Iterable
from functools import lru_cache

from boto_utils import emit_event

logger = logging.getLogger(__name__)


def emit_deletion_event(message_body, stats):
    job_id = message_body["JobId"]
    event_data = {
        "Statistics": stats,
        "Object": message_body["Object"],
    }
    emit_event(job_id, "ObjectUpdated", event_data, get_emitter_id())


def emit_failure_event(message_body, err_message, event_name):
    json_body = json.loads(message_body)
    job_id = json_body.get("JobId")
    if not job_id:
        raise ValueError("Message missing Job ID")
    event_data = {
        "Error": err_message,
        'Message': json_body,
    }
    emit_event(job_id, event_name, event_data, get_emitter_id())


def sanitize_message(err_message, message_body):
    """
    Obtain all the known match IDs from the original message and ensure
    they are masked in the given err message
    """
    try:
        sanitised = err_message
        if not isinstance(message_body, dict):
            message_body = json.loads(message_body)
        matches = []
        cols = message_body.get("Columns", [])
        for col in cols:
            match_ids = col.get("MatchIds")
            if isinstance(match_ids, Iterable):
                matches.extend(match_ids)
        for m in matches:
            sanitised = sanitised.replace(m, "*** MATCH ID ***")
        return sanitised
    except (json.decoder.JSONDecodeError, ValueError):
        return err_message


@lru_cache()
def get_emitter_id():
    metadata_file = os.getenv("ECS_CONTAINER_METADATA_FILE")
    if metadata_file and os.path.isfile(metadata_file):
        with open(metadata_file) as f:
            metadata = json.load(f)
        return "ECSTask_{}".format(metadata.get("TaskARN").rsplit("/", 1)[1])
    else:
        return "ECSTask"
