"""
Job Stats Updater
"""
import json
import logging

import boto3
from os import getenv
from collections import Counter

from boto_utils import DecimalEncoder, get_session, parse_s3_url
from s3 import revert_last

logger = logging.getLogger()
logger.setLevel(logging.INFO)
ddb = boto3.resource("dynamodb")
table = ddb.Table(getenv("JobTable", "S3F2_Jobs"))

deletion_queue_url = getenv("DeletionQueueUrl")


def update_stats(job_id, events):
    stats = _aggregate_stats(events)
    job = _update_job(job_id, stats)
    logger.info("Updated Stats for Job ID %s: %s", job_id, stats)
    return job


def _aggregate_stats(events):
    stats = Counter({})

    for event in events:
        event_name = event["EventName"]
        event_data = event.get("EventData", {})
        if event_name in ["QuerySucceeded", "QueryFailed"]:
            stats += Counter({
                "TotalQueryCount": 1,
                "TotalQuerySucceededCount": 1 if event_name == "QuerySucceeded" else 0,
                "TotalQueryFailedCount": 1 if event_name == "QueryFailed" else 0,
                "TotalQueryScannedInBytes": event_data.get("Statistics", {}).get("DataScannedInBytes", 0),
                "TotalQueryTimeInMillis": event_data.get("Statistics", {}).get("EngineExecutionTimeInMillis", 0),
            })
        if event_name in ["ObjectUpdated", "ObjectUpdateFailed", "ObjectRollbackFailed"]:
            stats += Counter({
                "TotalObjectUpdatedCount": 1 if event_name == "ObjectUpdated" else 0,
                "TotalObjectUpdateFailedCount": 1 if event_name == "ObjectUpdateFailed" else 0,
                "TotalObjectRollbackFailedCount": 1 if event_name == "ObjectRollbackFailed" else 0,
            })
            if event_name == "ObjectUpdateFailed":
            # TODO:: this can only work when deleteOldVersion flag is True for all involved data mappers
                try:
                    data_access_role_arn = event_data["Message"].get("RoleArn", getenv("DataAccessRoleArn"))
                    session = get_session(data_access_role_arn)
                    client = session.client("s3")
                    s3_object = event_data["Message"]["Object"]
                    input_bucket, input_key = parse_s3_url(s3_object)
                    revert_last(client, input_bucket, input_key)
                except Exception:
                    logger.exception("Unable to revert last")
                    try:
                        purge_queue(deletion_queue_url)
                    except Exception:
                        logger.exception("Unable to purge queue")

    return stats


def purge_queue(queue_url):
    sqs = boto3.resource("sqs")
    queue = sqs.Queue(queue_url)
    queue.purge()


def _update_job(job_id, stats):
    try:
        return table.update_item(
            Key={
                'Id': job_id,
                'Sk': job_id,
            },
            ConditionExpression="#Id = :Id AND #Sk = :Sk",
            UpdateExpression="set #qt = if_not_exists(#qt, :z) + :qt, "
                             "#qs = if_not_exists(#qs, :z) + :qs, "
                             "#qf = if_not_exists(#qf, :z) + :qf, "
                             "#qb = if_not_exists(#qb, :z) + :qb, "
                             "#qm = if_not_exists(#qm, :z) + :qm, "
                             "#ou = if_not_exists(#ou, :z) + :ou, "
                             "#of = if_not_exists(#of, :z) + :of, "
                             "#or = if_not_exists(#or, :z) + :or",
            ExpressionAttributeNames={
                "#Id": "Id",
                "#Sk": "Sk",
                '#qt': 'TotalQueryCount',
                '#qs': 'TotalQuerySucceededCount',
                '#qf': 'TotalQueryFailedCount',
                '#qb': 'TotalQueryScannedInBytes',
                '#qm': 'TotalQueryTimeInMillis',
                '#ou': 'TotalObjectUpdatedCount',
                '#of': 'TotalObjectUpdateFailedCount',
                '#or': 'TotalObjectRollbackFailedCount',
            },
            ExpressionAttributeValues={
                ":Id": job_id,
                ":Sk": job_id,
                ':qt': stats.get("TotalQueryCount", 0),
                ':qs': stats.get("TotalQuerySucceededCount", 0),
                ':qf': stats.get("TotalQueryFailedCount", 0),
                ':qb': stats.get("TotalQueryScannedInBytes", 0),
                ':qm': stats.get("TotalQueryTimeInMillis", 0),
                ':ou': stats.get("TotalObjectUpdatedCount", 0),
                ':of': stats.get("TotalObjectUpdateFailedCount", 0),
                ':or': stats.get("TotalObjectRollbackFailedCount", 0),
                ':z': 0,
            },
            ReturnValues="ALL_NEW"
        )["Attributes"]
    except ddb.meta.client.exceptions.ConditionalCheckFailedException:
        logger.warning("Job %s does not exist", job_id)
