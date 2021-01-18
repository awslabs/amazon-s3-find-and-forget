"""
Job Status Updater
"""
import json
import logging
import os

import boto3

from boto_utils import DecimalEncoder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ddb = boto3.resource("dynamodb")
table = ddb.Table(os.getenv("JobTable"))

status_map = {
    "FindPhaseFailed": "FIND_FAILED",
    "ForgetPhaseFailed": "FORGET_FAILED",
    "Exception": "FAILED",
    "JobStarted": "RUNNING",
    "ForgetPhaseEnded": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
    "CleanupFailed": "COMPLETED_CLEANUP_FAILED",
    "CleanupSucceeded": "COMPLETED",
}

unlocked_states = ["RUNNING", "QUEUED", "FORGET_COMPLETED_CLEANUP_IN_PROGRESS"]
skip_cleanup_states = [
    "FIND_FAILED",
    "FORGET_FAILED",
    "FAILED",
    "FORGET_PARTIALLY_FAILED",
]

time_statuses = {
    "JobStartTime": ["RUNNING"],
    "JobFinishTime": [
        "COMPLETED_CLEANUP_FAILED",
        "COMPLETED",
        "FAILED",
        "FIND_FAILED",
        "FORGET_FAILED",
        "FORGET_PARTIALLY_FAILED",
    ],
}


def update_status(job_id, events):
    attr_updates = {}
    for event in events:
        # Ignore non status events
        event_name = event["EventName"]
        if event_name not in status_map:
            if event_name == "QueryPlanningComplete":
                event_data = event.get("EventData", {})
                attr_updates["GeneratedQueries"] = event_data["GeneratedQueries"]
                attr_updates["DeletionQueueSize"] = event_data["DeletionQueueSize"]
                attr_updates["Manifests"] = event_data["Manifests"]
            continue

        new_status = determine_status(job_id, event_name)
        # Only change the status if it's still in an unlocked state
        if (
            not attr_updates.get("JobStatus")
            or attr_updates.get("JobStatus") in unlocked_states
        ):
            attr_updates["JobStatus"] = new_status

        # Update any job attributes
        for attr, statuses in time_statuses.items():
            if new_status in statuses and not attr_updates.get(attr):
                attr_updates[attr] = event["CreatedAt"]

    if len(attr_updates) > 0:
        job = _update_item(job_id, attr_updates)
        logger.info("Updated Status for Job ID %s: %s", job_id, attr_updates)
        return job


def determine_status(job_id, event_name):
    new_status = status_map[event_name]
    if event_name == "ForgetPhaseEnded" and job_has_errors(job_id):
        return "FORGET_PARTIALLY_FAILED"

    return new_status


def job_has_errors(job_id):
    item = table.get_item(Key={"Id": job_id, "Sk": job_id,}, ConsistentRead=True)[
        "Item"
    ]
    return (
        item.get("TotalObjectUpdateFailedCount", 0) > 0
        or item.get("TotalQueryFailedCount") > 0
    )


def _update_item(job_id, attr_updates):
    try:
        update_expression = "set " + ", ".join(
            ["#{k} = :{k}".format(k=k) for k, v in attr_updates.items()]
        )
        attr_names = {}
        attr_values = {}

        for k, v in attr_updates.items():
            attr_names["#{}".format(k)] = k
            attr_values[":{}".format(k)] = v

        unlocked_states_condition = " OR ".join(
            ["#JobStatus = :{}".format(s) for s in unlocked_states]
        )

        return table.update_item(
            Key={"Id": job_id, "Sk": job_id,},
            UpdateExpression=update_expression,
            ConditionExpression="#Id = :Id AND #Sk = :Sk AND ({})".format(
                unlocked_states_condition
            ),
            ExpressionAttributeNames={
                "#Id": "Id",
                "#Sk": "Sk",
                "#JobStatus": "JobStatus",
                **attr_names,
            },
            ExpressionAttributeValues={
                ":Id": job_id,
                ":Sk": job_id,
                **{":{}".format(s): s for s in unlocked_states},
                **attr_values,
            },
            ReturnValues="ALL_NEW",
        )["Attributes"]
    except ddb.meta.client.exceptions.ConditionalCheckFailedException:
        logger.warning("Job %s is already in a status which cannot be updated", job_id)
