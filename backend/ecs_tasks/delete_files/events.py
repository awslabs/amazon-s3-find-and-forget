import json
import os
import logging
import urllib.request
import urllib.error
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


def emit_skipped_event(message_body, skip_reason):
    job_id = message_body["JobId"]
    event_data = {
        "Object": message_body["Object"],
        "Reason": skip_reason,
    }
    emit_event(job_id, "ObjectUpdateSkipped", event_data, get_emitter_id())


def emit_failure_event(message_body, err_message, event_name):
    json_body = json.loads(message_body)
    job_id = json_body.get("JobId")
    if not job_id:
        raise ValueError("Message missing Job ID")
    event_data = {
        "Error": err_message,
        "Message": json_body,
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
            sanitised = sanitised.replace(str(m), "*** MATCH ID ***")
        return sanitised
    except (json.decoder.JSONDecodeError, ValueError):
        return err_message


@lru_cache()
def get_emitter_id():
    metadata_endpoint = os.getenv("ECS_CONTAINER_METADATA_URI")
    if metadata_endpoint:
        res = ""
        try:
            res = urllib.request.urlopen(metadata_endpoint, timeout=1).read()
            metadata = json.loads(res)
            return "ECSTask_{}".format(
                metadata["Labels"]["com.amazonaws.ecs.task-arn"].rsplit("/", 1)[1]
            )
        except urllib.error.URLError as e:
            logger.warning(
                "Error when accessing the metadata service: {}".format(e.reason)
            )
        except (AttributeError, KeyError, IndexError) as e:
            logger.warning(
                "Malformed response from the metadata service: {}".format(res)
            )
        except Exception as e:
            logger.warning(
                "Error when getting emitter id from metadata service: {}".format(str(e))
            )

    return "ECSTask"
