import json
import logging
from os import getenv
from pathlib import Path
from urllib.parse import urljoin
from uuid import uuid4

import boto3
import pytest
from aws_xray_sdk import global_sdk_config
from botocore.exceptions import ClientError
from botocore.waiter import WaiterModel, create_waiter_with_client
from requests import Session

from . import load_env, empty_table

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
def stack():
    cloudformation = boto3.resource('cloudformation')
    stack = cloudformation.Stack(getenv("StackName", "amazon-s3-find-and-forget"))
    return {o["OutputKey"]: o["OutputValue"] for o in stack.outputs}


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


@pytest.fixture(scope="session", autouse=True)
def cognito_token(stack):
    # Generate User in Cognito
    user_pool_id = stack["CognitoUserPoolId"]
    client_id = stack["CognitoUserPoolClientId"]
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
def api_client(cognito_token, stack):
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

    return ApiGwSession(stack["ApiUrl"], hds)


@pytest.fixture(scope="module")
def queue_base_endpoint():
    return "queue"


@pytest.fixture(scope="module")
def queue_table(ddb_resource, stack):
    return ddb_resource.Table(stack["DeletionQueueTable"])


@pytest.fixture(scope="module")
def empty_queue(queue_table):
    empty_table(queue_table, "MatchId")


@pytest.fixture
def del_queue_factory(queue_table):
    def factory(match_id="testId", data_mappers=[]):
        item = {
            "MatchId": match_id,
            "DataMappers": data_mappers,
        }
        queue_table.put_item(Item=item)
        return item

    yield factory

    empty_table(queue_table, "MatchId")


@pytest.fixture(scope="module")
def data_mapper_base_endpoint():
    return "data_mappers"


@pytest.fixture(scope="module")
def data_mapper_table(ddb_resource, stack):
    return ddb_resource.Table(stack["DataMapperTable"])


@pytest.fixture(scope="module")
def empty_data_mappers(data_mapper_table):
    empty_table(data_mapper_table, "DataMapperId")


@pytest.fixture()
def glue_data_mapper_factory(dummy_lake, glue_client, data_mapper_table):
    """
    Factory for registering a data mapper in DDB and createing a corresponding glue table
    """
    items = []
    bucket_name = dummy_lake["bucket_name"]

    def factory(data_mapper_id="test", columns=["customer_id"], fmt="parquet", database="acceptancetests",
                table="acceptancetests", partition_keys=[], partitions=[]):
        item = {
            "DataMapperId": data_mapper_id,
            "Columns": columns,
            "QueryExecutor": "athena",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": database,
                "Table": table
            },
            "Format": fmt,
        }
        data_mapper_table.put_item(Item=item)
        glue_client.create_database(DatabaseInput={'Name': database})
        glue_client.create_table(
            DatabaseName=database,
            TableInput={
                "Name": table,
                "StorageDescriptor": {
                    "Columns": [{
                        'Name': col,
                        'Type': 'string',
                    } for col in columns],
                    "Location": "s3://{bucket}/{prefix}/".format(bucket=bucket_name, prefix=data_mapper_id),
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "Compressed": False,
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {
                            "serialization.format": "1"
                        }
                    },
                    "StoredAsSubDirectories": False
                },
                'PartitionKeys': [
                    {
                        'Name': pk,
                        'Type': 'string',
                    } for pk in partition_keys
                ],
                "Parameters": {
                    "EXTERNAL": "TRUE"
                }
            }
        )
        for p in partitions:
            glue_client.create_partition(
                DatabaseName=database,
                TableName=table,
                PartitionInput={
                    'Values': p,
                    'StorageDescriptor': {
                        "Columns": [{
                            'Name': col,
                            'Type': 'string',
                        } for col in columns],
                        'Location': "s3://{bucket}/{prefix}/{parts}/".format(bucket=bucket_name, prefix=data_mapper_id,
                                                                             parts="/".join(p)),
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            "Parameters": {
                                "serialization.format": "1"
                            }
                        },
                    },
                }
            )

        items.append(item)
        return item

    yield factory

    empty_table(data_mapper_table, "DataMapperId")
    for i in items:
        db_name = i["QueryExecutorParameters"]["Database"]
        table_name = i["QueryExecutorParameters"]["Table"]
        glue_client.delete_table(
            DatabaseName=db_name,
            Name=table_name
        )
        glue_client.delete_database(Name=db_name)


@pytest.fixture(scope="module")
def jobs_endpoint():
    return "jobs"


@pytest.fixture(scope="session")
def execution_waiter(sf_client):
    waiter_dir = Path(__file__).parent.parent.joinpath("waiters")
    with open(waiter_dir.joinpath("stepfunctions.json")) as f:
        config = json.load(f)

    waiter_model = WaiterModel(config)
    return create_waiter_with_client("ExecutionComplete", waiter_model, sf_client)


@pytest.fixture(scope="function")
def execution(sf_client, stack):
    """
    Generates a sample index config in the db which is cleaned up after the test
    """
    response = sf_client.start_execution(
        stateMachineArn=stack["StateMachineArn"]
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
def dummy_lake(s3_resource, stack):
    # Lake Config
    bucket_name = "test-" + str(uuid4())
    # Create the bucket and Glue table
    bucket = s3_resource.Bucket(bucket_name)
    policy = s3_resource.BucketPolicy(bucket_name)
    bucket.create(CreateBucketConfiguration={
        "LocationConstraint": getenv("AWS_DEFAULT_REGION", "eu-west-1")
    }, )
    bucket.wait_until_exists()
    policy.put(Policy=json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": [
                        stack["AthenaExecutionRoleArn"]
                    ]
                },
                "Action": "s3:*",
                "Resource": [
                    "arn:aws:s3:::{}".format(bucket_name),
                    "arn:aws:s3:::{}/*".format(bucket_name),
                ]
            }
        ]
    }))

    yield {
        "bucket_name": bucket_name,
        "bucket": bucket,
    }

    # Cleanup
    bucket.objects.delete()
    bucket.delete()


@pytest.fixture(scope="session")
def data_loader(dummy_lake):
    def load_data(filename, object_key):
        bucket = dummy_lake["bucket"]
        file_path = str(Path(__file__).parent.joinpath("data").joinpath(filename))
        bucket.upload_file(file_path, object_key)

    return load_data
