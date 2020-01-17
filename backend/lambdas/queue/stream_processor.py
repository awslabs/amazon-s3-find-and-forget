import logging
from os import getenv
import json
import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

from boto_utils import DecimalEncoder

deserializer = TypeDeserializer()
logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('stepfunctions')
state_machine_arn = getenv("StateMachineArn")


def handler(event, context):
    """
    Executes a state machine in response to a record being written to a
    DynamoDB table
    """
    logger.info(event)
    records = [
        r for r in event["Records"] if should_process(r)
    ]
    for record in records:
        new_image = record["dynamodb"]["NewImage"]
        deserialized = {}
        for key in new_image:
            deserialized[key] = deserializer.deserialize(new_image[key])
        job_id = deserialized["Id"]
        try:
            client.start_execution(
                stateMachineArn=state_machine_arn,
                name=job_id,
                input=json.dumps(deserialized, cls=DecimalEncoder)
            )
        except client.exceptions.ExecutionAlreadyExists:
            logger.warning("Execution {} already exists".format(job_id))


def should_process(record):
    return record.get("eventName") == "INSERT"

