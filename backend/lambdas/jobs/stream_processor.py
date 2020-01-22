import logging
from os import getenv
import json
import boto3
from boto3.dynamodb.types import TypeDeserializer
from itertools import groupby
from operator import itemgetter

from stats_updater import update_stats
from status_updater import update_status
from boto_utils import DecimalEncoder
from decorators import with_logger

deserializer = TypeDeserializer()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('stepfunctions')
state_machine_arn = getenv("StateMachineArn")


@with_logger
def handler(event, context):
    jobs = [
        deserialize_item(r) for r in event["Records"] if should_process(r) and is_job(r)
    ]
    events = [
        deserialize_item(r) for r in event["Records"] if should_process(r) and is_job_event(r)
    ]
    grouped_events = groupby(sorted(events, key=itemgetter("Id")), key=itemgetter("Id"))

    for job in jobs:
        process_job(job)

    for job_id, group in grouped_events:
        status = update_status(job_id, group)
        context.logger.info("Updated Status for Job ID {}: {}".format(job_id, status))
        stats = update_stats(job_id, group)
        context.logger.info("Updated Stats for Job ID {}: {}".format(job_id, json.dumps(stats)))


def deserialize_item(record):
    new_image = record["dynamodb"]["NewImage"]
    deserialized = {}
    for key in new_image:
        deserialized[key] = deserializer.deserialize(new_image[key])
    return deserialized


def process_job(job):
    job_id = job["Id"]
    try:
        client.start_execution(
            stateMachineArn=state_machine_arn,
            name=job_id,
            input=json.dumps(job, cls=DecimalEncoder)
        )
    except client.exceptions.ExecutionAlreadyExists:
        logger.warning("Execution {} already exists".format(job_id))


def should_process(record):
    return record.get("eventName") == "INSERT"


def is_job(record):
    return deserializer.deserialize(record["dynamodb"]["NewImage"]["Type"]) == "Job"


def is_job_event(record):
    return deserializer.deserialize(record["dynamodb"]["NewImage"]["Type"]) == "JobEvent"
