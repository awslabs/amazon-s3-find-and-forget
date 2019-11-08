import json
import logging
from functools import partial
from os import getenv
from pathlib import Path
from urllib.parse import urljoin

import boto3
import pytest
from aws_xray_sdk import global_sdk_config
from botocore.exceptions import ClientError
from botocore.waiter import WaiterModel, create_waiter_with_client
from requests import Session
from uuid import uuid4

from . import load_env

logger = logging.getLogger()


#########
# HOOKS #
#########

def pytest_configure(config):
    """
    Initial test env setup
    """
    global_sdk_config.set_sdk_enabled(False)
    load_env()


def pytest_unconfigure(config):
    """
    Teardown actions
    """
    pass


############
# FIXTURES #
############


@pytest.fixture(scope="session")
def ddb_resource():
    return boto3.resource("dynamodb")


@pytest.fixture(scope="session")
def s3_resource():
    return boto3.resource("s3")


@pytest.fixture(scope="session")
def sf_client():
    return boto3.client("stepfunctions")


@pytest.fixture(scope="session")
def glue_client():
    return boto3.client("glue")


@pytest.fixture(scope="session")
def get_table_name():
    def func(prefix, name):
        return "{}_{}".format(prefix, name)

    return partial(func, getenv("TablePrefix"))


@pytest.fixture(scope="session", autouse=True)
def cognito_token():
    # Generate User in Cognito
    user_pool_id = getenv("UserPoolId")
    client_id = getenv("ClientId")
    username = "aws-uk-sa-builders@amazon.com"
    pwd = "acceptance-tests-password"
    auth_data = {"USERNAME": username, "PASSWORD": pwd}
    provider_client = boto3.client("cognito-idp")
    # Create the User
    provider_client.admin_create_user(
        UserPoolId=user_pool_id,
        Username=username,
        TemporaryPassword=pwd,
        MessageAction="SUPPRESS",
    )
    provider_client.admin_set_user_password(
        UserPoolId=user_pool_id,
        Username=username,
        Password=pwd,
        Permanent=True
    )
    # Allow admin login
    provider_client.update_user_pool_client(
        UserPoolId=user_pool_id,
        ClientId=client_id,
        ExplicitAuthFlows=[
            "ADMIN_NO_SRP_AUTH",
        ],
    )
    # Get JWT token for the dummy user
    resp = provider_client.admin_initiate_auth(UserPoolId=user_pool_id, AuthFlow="ADMIN_NO_SRP_AUTH",
                                               AuthParameters=auth_data, ClientId=client_id)
    yield resp["AuthenticationResult"]["IdToken"]
    provider_client.admin_delete_user(
        UserPoolId=user_pool_id,
        Username=username
    )


@pytest.fixture(scope="session")
def api_client(cognito_token):
    class ApiGwSession(Session):
        def __init__(self, base_url=None, default_headers=None):
            if default_headers is None:
                default_headers = {}
            self.base_url = base_url
            self.default_headers = default_headers
            super(ApiGwSession, self).__init__()

        def request(self, method, url, data=None, params=None, headers=None, *args, **kwargs):
            url = urljoin(self.base_url, url)
            if isinstance(headers, dict):
                self.default_headers.update(headers)
            return super(ApiGwSession, self).request(
                method, url, data, params, headers=self.default_headers, *args, **kwargs
            )

    hds = {
        "Content-Type": "application/json"
    }
    if cognito_token:
        hds.update({
            "Authorization": "Bearer {}".format(cognito_token)
        })

    return ApiGwSession(getenv("ApiUrl"), hds)


@pytest.fixture(scope="module")
def queue_base_endpoint():
    return "queue"


@pytest.fixture(scope="module")
def queue_table(ddb_resource, get_table_name):
    return ddb_resource.Table(get_table_name("DeletionQueue"))


@pytest.fixture(scope="module")
def empty_queue(queue_table):
    items = queue_table.scan()["Items"]
    with queue_table.batch_writer() as batch:
        for item in items:
            batch.delete_item(
                Key={
                    "MatchId": item["MatchId"],
                }
            )


@pytest.fixture
def del_queue_item(queue_table, match_id="testId", data_mappers=[]):
    item = {
        "MatchId": match_id,
        "DataMappers": data_mappers,
    }
    queue_table.put_item(Item=item)
    yield item
    queue_table.delete_item(Key={
        "MatchId": match_id
    })


@pytest.fixture(scope="module")
def data_mapper_base_endpoint():
    return "data_mappers"


@pytest.fixture(scope="module")
def data_mapper_table(ddb_resource, get_table_name):
    return ddb_resource.Table(get_table_name("DataMappers"))


@pytest.fixture()
def glue_data_mapper_item(data_mapper_table, data_mapper_id="test", columns=["test"], fmt="parquet"):
    item = {
        "DataMapperId": data_mapper_id,
        "Columns": columns,
        "DataSource": {
            "Type": "glue",
            "Parameters": {
                "Database": "acceptancetestsdb",
                "Table": "acceptancetests"
            }
        },
        "Format": fmt,
    }
    data_mapper_table.put_item(Item=item)
    yield item
    data_mapper_table.delete_item(Key={
        "DataMapperId": data_mapper_id
    })


@pytest.fixture(scope="module")
def jobs_endpoint():
    return "jobs"


@pytest.fixture
def state_machine(sf_client):
    yield {
        "stateMachineArn": getenv("StateMachineArn")
    }


@pytest.fixture(scope="session")
def execution_waiter(sf_client):
    waiter_dir = Path(__file__).parent.parent.joinpath("waiters")
    with open(waiter_dir.joinpath("stepfunctions.json")) as f:
        config = json.load(f)

    waiter_model = WaiterModel(config)
    return create_waiter_with_client("ExecutionComplete", waiter_model, sf_client)


@pytest.fixture(scope="function")
def execution(sf_client, state_machine):
    """
    Generates a sample index config in the db which is cleaned up after the test
    """
    response = sf_client.start_execution(
        stateMachineArn=state_machine["stateMachineArn"]
    )
    yield response
    try:
        sf_client.stop_execution(executionArn=response["executionArn"])
    except ClientError as e:
        logger.warning("Error stopping state machine: %s", str(e))


@pytest.fixture(scope="module")
def empty_lake(dummy_lake):
    dummy_lake["bucket"].objects.delete()


@pytest.fixture(scope="session")
def dummy_lake(s3_resource, glue_client):
    # TODO: Only supports setting up a glue catalogued database
    # Lake Config
    bucket_name = "test-" + str(uuid4())
    db_name = getenv("DatabaseName")
    table_name = "acceptancetests"
    prefix = str(uuid4())
    # Create the bucket and Glue table
    bucket = s3_resource.Bucket(bucket_name)
    bucket.create(CreateBucketConfiguration={
        "LocationConstraint": getenv("AWS_DEFAULT_REGION", "eu-west-1")
    },)
    bucket.wait_until_exists()
    glue_client.create_table(
        DatabaseName=db_name,
        TableInput={
            "Name": table_name,
            "StorageDescriptor": {
                "Columns": [{
                    'Name': 'customer_id',
                    'Type': 'string',
                }],
                "Location": "s3://{bucket}/{prefix}/".format(bucket=bucket_name, prefix=prefix),
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "Compressed": False,
                "NumberOfBuckets": -1,
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {
                        "serialization.format": "1"
                    }
                },
                "BucketColumns": [],
                "SortColumns": [],
                "Parameters": {},
                "SkewedInfo": {
                    "SkewedColumnNames": [],
                    "SkewedColumnValues": [],
                    "SkewedColumnValueLocationMaps": {}
                },
                "StoredAsSubDirectories": False
            },
            "Parameters": {
                "EXTERNAL": "TRUE",
                "S3FindAndForgetColumns": "customer_id"
            }
        }
    )
    # TODO: Partition

    yield {
        "bucket_name": bucket_name,
        "prefix": prefix,
        "bucket": bucket,
        "table_name": table_name,
    }

    # Cleanup
    glue_client.delete_table(
        DatabaseName=db_name,
        Name=table_name
    )
    bucket.objects.delete()
    bucket.delete()
    bucket.wait_until_not_exists()

