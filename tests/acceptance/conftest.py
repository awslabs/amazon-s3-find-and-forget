import json
import logging
from os import path, getenv

import boto3
import botocore
import pytest
from cfn_flip import load

logger = logging.getLogger()
# ENV SETUP
running_local_resources = getenv("RUN_LOCAL", True)
CONFIGURATION_TABLE_NAME = getenv("CONFIGURATION_TABLE_NAME", "JaneDoe_Configuration")
MATCHES_TABLE_NAME = getenv("MATCHES_TABLE_NAME", "JaneDoe_Matches")
OBJECT_STATE_TABLE_NAME = getenv("OBJECT_STATE_TABLE_NAME", "JaneDoe_ObjectStates")
LAMBDA_ENDPOINT = getenv("LAMBDA_ENDPOINT", "http://127.0.0.1:3001")
DDB_ENDPOINT = getenv("DDB_ENDPOINT", "http://127.0.0.1:8000")
REGION = "eu-west-1"
PROJECT_ROOT = path.dirname(path.dirname(path.dirname(__file__)))
# LOAD DEFINITIONS
with open(path.join(PROJECT_ROOT, "templates", "ddb.yaml")) as f:
    ddb_template = load(f.read())[0]

# Setup DDB local
kwargs = {}
if running_local_resources:
    session = boto3.Session(
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
    )
    kwargs["endpoint_url"] = DDB_ENDPOINT
else:
    session = boto3.Session()

ddb = session.resource('dynamodb', **kwargs)


def create_table(table_name, hash_key, hash_key_type='S', range_key=None, range_key_type='S', attributes=[]):
    """
    Creates a DDB table for testing
    """
    key_schema = [{
        'AttributeName': hash_key,
        'KeyType': 'HASH'
    }, ]
    attr_definitions = [
        {
            'AttributeName': hash_key,
            'AttributeType': hash_key_type
        },
    ]
    if range_key:
        key_schema.append({
            'AttributeName': range_key,
            'KeyType': 'RANGE'
        })
        attr_definitions.append({
            'AttributeName': range_key,
            'AttributeType': range_key_type
        })
    attr_definitions = attr_definitions + attributes
    t = ddb.create_table(
        AttributeDefinitions=attr_definitions,
        TableName=table_name,
        KeySchema=key_schema,
        BillingMode='PAY_PER_REQUEST',
    )
    logger.info("Table %s: %s", table_name, t.table_status)


def delete_table(table_name):
    """
    Deletes a DDB table for testing
    """
    table = ddb.Table(table_name)
    table.delete()


def get_schema_from_template(logical_identifier):
    resource = ddb_template["Resources"].get(logical_identifier)
    if not resource:
        raise KeyError("Unable to find resource with identifier %s", logical_identifier)

    return {
        k["KeyType"]: k["AttributeName"] for k in resource["Properties"]["KeySchema"]
    }


def pytest_configure(config):
    """
    Initial test env setup
    """
    logger.info("Starting acceptance tests")
    if running_local_resources:
        logger.info("Setting up DynamoDB tables...")
        config_schema = get_schema_from_template("ConfigurationTable")
        matches_schema = get_schema_from_template("MatchesTable")
        object_state_table = get_schema_from_template("ObjectsStateTable")
        create_table(CONFIGURATION_TABLE_NAME, hash_key=config_schema["HASH"])
        create_table(MATCHES_TABLE_NAME, hash_key=matches_schema["HASH"], range_key=matches_schema["RANGE"])
        create_table(OBJECT_STATE_TABLE_NAME, hash_key=object_state_table["HASH"])
        logger.info("DynamoDB tables ready!")


def pytest_unconfigure(config):
    """
    Teardown actions
    """
    if running_local_resources:
        try:
            logger.info("Clearing down DynamoDB tables...")
            delete_table(CONFIGURATION_TABLE_NAME)
            delete_table(MATCHES_TABLE_NAME)
            delete_table(OBJECT_STATE_TABLE_NAME)
            logger.info("DynamoDB tables cleared!")
        except Exception as e:
            logger.error(str(e))
            logger.warning("Unable to clear DynamoDB tables. Remove them before next run if not using -inMemory")


@pytest.fixture
def lambda_client():
    """
    Creates a Lambda client for use locally
    """
    if running_local_resources:
        client_config = botocore.client.Config(
            signature_version=botocore.UNSIGNED, retries={'max_attempts': 0}, )

        return boto3.client('lambda', endpoint_url=LAMBDA_ENDPOINT, verify=False, config=client_config)
    return boto3.client('lambda')


@pytest.fixture
def dynamodb_resource():
    """
    Creates a DDB resource for use locally
    """
    return ddb


@pytest.fixture
def index_config(s3_uri="s3://test_bucket/test_path/", object_types="parquet", columns=["user_id"], s3_trigger=True):
    """
    Generates a sample index config in the db which is cleaned up after the test
    """
    table = ddb.Table(CONFIGURATION_TABLE_NAME)
    item = {
        "S3Uri": s3_uri,
        "S3Trigger": s3_trigger,
        "Columns": columns,
        "ObjectTypes": object_types
    }
    table.put_item(Item=item)
    yield item
    table.delete_item(Key={
        "S3Uri": s3_uri
    })


@pytest.fixture
def event_generator():
    def load_file(event_type, key_overrides={}):
        events_path = path.join(path.dirname(__file__), 'events')
        file_path = path.join(events_path, event_type + ".json")

        with open(file_path) as f:
            data = json.load(f)
            data.update(key_overrides)

            return data

    return load_file
