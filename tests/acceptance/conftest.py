import datetime
import json
import logging
from copy import deepcopy
from os import getenv
from pathlib import Path
from urllib.parse import urljoin
from uuid import uuid4

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.waiter import WaiterModel, create_waiter_with_client
from requests import Session

from . import empty_table

logger = logging.getLogger()


#########
# HOOKS #
#########


def pytest_configure(config):
    """
    Initial test env setup
    """
    pass


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
    """
    List cloudformation stack

    Args:
    """
    cloudformation = boto3.resource("cloudformation")
    stack = cloudformation.Stack(getenv("StackName", "S3F2"))
    return {o["OutputKey"]: o["OutputValue"] for o in stack.outputs}


@pytest.fixture
def config(stack):
    """
    Configure cloudformation.

    Args:
        stack: (list): write your description
    """
    ssm = boto3.client("ssm")
    return json.loads(
        ssm.get_parameter(Name=stack["ConfigParameter"], WithDecryption=True)[
            "Parameter"
        ]["Value"]
    )


@pytest.fixture
def config_mutator(config, ssm_client, stack):
    """
    Context manager for the mutator.

    Args:
        config: (dict): write your description
        ssm_client: (todo): write your description
        stack: (list): write your description
    """
    ssm = boto3.client("ssm")

    def mutator(**kwargs):
        """
        Mutate a mutator.

        Args:
        """
        tmp = {**config, **kwargs}
        ssm.put_parameter(
            Name=stack["ConfigParameter"],
            Value=json.dumps(tmp),
            Type="String",
            Overwrite=True,
        )

    yield mutator
    ssm.put_parameter(
        Name=stack["ConfigParameter"],
        Value=json.dumps(config),
        Type="String",
        Overwrite=True,
    )


@pytest.fixture(scope="session")
def ddb_resource():
    """
    Return the boto resource.

    Args:
    """
    return boto3.resource("dynamodb")


@pytest.fixture(scope="session")
def ddb_client():
    """
    Return a boto3 client.

    Args:
    """
    return boto3.client("dynamodb")


@pytest.fixture(scope="session")
def s3_resource():
    """
    Return a s3 resource.

    Args:
    """
    return boto3.resource("s3")


@pytest.fixture(scope="session")
def sf_client():
    """
    Return a boto3 client.

    Args:
    """
    return boto3.client("stepfunctions")


@pytest.fixture(scope="session")
def glue_client():
    """
    Glue client

    Args:
    """
    return boto3.client("glue")


@pytest.fixture(scope="session")
def ssm_client():
    """
    Return a boto client

    Args:
    """
    return boto3.client("ssm")


@pytest.fixture(scope="session")
def iam_client():
    """
    Return a boto client

    Args:
    """
    return boto3.client("iam")


@pytest.fixture(scope="session")
def glue_columns():
    """
    Return a dict of columns.

    Args:
    """
    return [
        {"Name": "customer_id", "Type": "string"},
        {"Name": "user_info", "Type": "struct<email:string,name:string>"},
        {"Name": "days_off", "Type": "array<string>"},
    ]


@pytest.fixture(scope="session", autouse=True)
def cognito_token(stack):
    """
    Cognito provider

    Args:
        stack: (list): write your description
    """
    # Generate User in Cognito
    user_pool_id = stack["CognitoUserPoolId"]
    client_id = stack["CognitoUserPoolClientId"]
    username = "aws-uk-sa-builders@amazon.com"
    pwd = "!Acceptance1Tests2password!"
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
        UserPoolId=user_pool_id, Username=username, Password=pwd, Permanent=True
    )
    # Allow admin login
    provider_client.update_user_pool_client(
        UserPoolId=user_pool_id,
        ClientId=client_id,
        ExplicitAuthFlows=["ADMIN_NO_SRP_AUTH",],
    )
    # Get JWT token for the dummy user
    resp = provider_client.admin_initiate_auth(
        UserPoolId=user_pool_id,
        AuthFlow="ADMIN_NO_SRP_AUTH",
        AuthParameters=auth_data,
        ClientId=client_id,
    )
    yield resp["AuthenticationResult"]["IdToken"]
    provider_client.admin_delete_user(UserPoolId=user_pool_id, Username=username)


@pytest.fixture(scope="session")
def api_client(cognito_token, stack):
    """
    Initialize an api session.

    Args:
        cognito_token: (str): write your description
        stack: (list): write your description
    """
    class ApiGwSession(Session):
        def __init__(self, base_url=None, default_headers=None):
            """
            Initialize the base class.

            Args:
                self: (todo): write your description
                base_url: (str): write your description
                default_headers: (str): write your description
            """
            if default_headers is None:
                default_headers = {}
            self.base_url = base_url
            self.default_headers = default_headers
            super(ApiGwSession, self).__init__()

        def request(
            self, method, url, data=None, params=None, headers=None, *args, **kwargs
        ):
            """
            Make a http request.

            Args:
                self: (todo): write your description
                method: (str): write your description
                url: (str): write your description
                data: (str): write your description
                params: (dict): write your description
                headers: (dict): write your description
            """
            url = urljoin("{}/v1/".format(self.base_url), url)
            merged_headers = deepcopy(self.default_headers)
            if isinstance(headers, dict):
                merged_headers.update(headers)
            return super(ApiGwSession, self).request(
                method, url, data, params, headers=merged_headers, *args, **kwargs
            )

    hds = {"Content-Type": "application/json"}
    if cognito_token:
        hds.update({"Authorization": "Bearer {}".format(cognito_token)})

    return ApiGwSession(stack["ApiUrl"], hds)


@pytest.fixture(scope="module")
def queue_base_endpoint():
    """
    Return the endpoint endpoint.

    Args:
    """
    return "queue"


@pytest.fixture(scope="module")
def settings_base_endpoint():
    """
    Return the endpoint url.

    Args:
    """
    return "settings"


@pytest.fixture(scope="module")
def queue_table(ddb_resource, stack):
    """
    Queue a ddl table.

    Args:
        ddb_resource: (todo): write your description
        stack: (list): write your description
    """
    return ddb_resource.Table(stack["DeletionQueueTable"])


@pytest.fixture
def del_queue_factory(queue_table):
    """
    Deletes a queue.

    Args:
        queue_table: (todo): write your description
    """
    def factory(
        match_id="testId",
        created_at=round(datetime.datetime.now(datetime.timezone.utc).timestamp()),
        data_mappers=[],
        deletion_queue_item_id="id123",
    ):
        """
        Create a new item

        Args:
            match_id: (int): write your description
            created_at: (bool): write your description
            round: (array): write your description
            datetime: (todo): write your description
            datetime: (todo): write your description
            now: (todo): write your description
            datetime: (todo): write your description
            timezone: (todo): write your description
            utc: (array): write your description
            timestamp: (int): write your description
            data_mappers: (todo): write your description
            deletion_queue_item_id: (int): write your description
        """
        item = {
            "DeletionQueueItemId": deletion_queue_item_id,
            "MatchId": match_id,
            "CreatedAt": created_at,
            "DataMappers": data_mappers,
        }
        queue_table.put_item(Item=item)
        return item

    yield factory

    empty_table(queue_table, "DeletionQueueItemId")


@pytest.fixture(scope="module")
def data_mapper_base_endpoint():
    """
    Return the endpoint endpoint.

    Args:
    """
    return "data_mappers"


@pytest.fixture(scope="module")
def data_mapper_table(ddb_resource, stack):
    """
    Convert a ddl table.

    Args:
        ddb_resource: (todo): write your description
        stack: (list): write your description
    """
    return ddb_resource.Table(stack["DataMapperTable"])


@pytest.fixture(scope="function")
def empty_data_mappers(data_mapper_table):
    """
    Yields all the mapper.

    Args:
        data_mapper_table: (todo): write your description
    """
    empty_table(data_mapper_table, "DataMapperId")
    yield
    empty_table(data_mapper_table, "DataMapperId")


@pytest.fixture
def glue_table_factory(dummy_lake, glue_client, glue_columns):
    """
    Builds a database table.

    Args:
        dummy_lake: (todo): write your description
        glue_client: (todo): write your description
        glue_columns: (bool): write your description
    """
    items = []
    bucket_name = dummy_lake["bucket_name"]

    def factory(
        columns=glue_columns,
        fmt="parquet",
        database="acceptancetests",
        table="acceptancetests",
        prefix="prefix",
        partition_keys=[],
        partitions=[],
        partition_key_types="string",
    ):
        """
        Creates database table.

        Args:
            columns: (list): write your description
            glue_columns: (bool): write your description
            fmt: (array): write your description
            database: (str): write your description
            table: (str): write your description
            prefix: (str): write your description
            partition_keys: (str): write your description
            partitions: (list): write your description
            partition_key_types: (str): write your description
        """
        glue_client.create_database(DatabaseInput={"Name": database})
        input_format = (
            "org.apache.hadoop.mapred.TextInputFormat"
            if fmt == "json"
            else "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        )
        output_format = (
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
            if fmt == "json"
            else "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
        )
        ser_library = (
            "org.openx.data.jsonserde.JsonSerDe"
            if fmt == "json"
            else "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        )
        glue_client.create_table(
            DatabaseName=database,
            TableInput={
                "Name": table,
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": "s3://{bucket}/{prefix}/".format(
                        bucket=bucket_name, prefix=prefix
                    ),
                    "InputFormat": input_format,
                    "OutputFormat": output_format,
                    "Compressed": False,
                    "SerdeInfo": {
                        "SerializationLibrary": ser_library,
                        "Parameters": {"serialization.format": "1"},
                    },
                    "StoredAsSubDirectories": False,
                },
                "PartitionKeys": [
                    {"Name": pk, "Type": partition_key_types} for pk in partition_keys
                ],
                "Parameters": {"EXTERNAL": "TRUE"},
            },
        )

        for p in partitions:
            glue_client.create_partition(
                DatabaseName=database,
                TableName=table,
                PartitionInput={
                    "Values": p,
                    "StorageDescriptor": {
                        "Columns": columns,
                        "Location": "s3://{bucket}/{prefix}/{parts}/".format(
                            bucket=dummy_lake["bucket_name"],
                            prefix=prefix,
                            parts="/".join(p),
                        ),
                        "InputFormat": input_format,
                        "OutputFormat": output_format,
                        "SerdeInfo": {
                            "SerializationLibrary": ser_library,
                            "Parameters": {"serialization.format": "1"},
                        },
                    },
                },
            )
        item = {"Database": database, "Table": table}
        items.append(item)
        return item

    yield factory

    for i in items:
        db_name = i["Database"]
        table_name = i["Table"]
        glue_client.delete_table(DatabaseName=db_name, Name=table_name)
        glue_client.delete_database(Name=db_name)


@pytest.fixture
def glue_data_mapper_factory(
    glue_client, data_mapper_table, glue_table_factory, glue_columns
):
    """
    Factory for registering a data mapper in DDB and createing a corresponding glue table
    """
    items = []

    def factory(
        data_mapper_id="test",
        columns=glue_columns,
        fmt="parquet",
        database="acceptancetests",
        table="acceptancetests",
        partition_keys=[],
        partitions=[],
        role_arn=None,
        delete_old_versions=False,
        column_identifiers=["customer_id"],
        partition_key_types="string",
    ):
        """
        Return a table factory.

        Args:
            data_mapper_id: (str): write your description
            columns: (list): write your description
            glue_columns: (bool): write your description
            fmt: (array): write your description
            database: (str): write your description
            table: (str): write your description
            partition_keys: (str): write your description
            partitions: (list): write your description
            role_arn: (str): write your description
            delete_old_versions: (bool): write your description
            column_identifiers: (str): write your description
            partition_key_types: (str): write your description
        """
        item = {
            "DataMapperId": data_mapper_id,
            "Columns": column_identifiers,
            "QueryExecutor": "athena",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": database,
                "Table": table,
            },
            "Format": fmt,
            "DeleteOldVersions": delete_old_versions,
        }
        if role_arn:
            item["RoleArn"] = role_arn
        data_mapper_table.put_item(Item=item)
        glue_table_factory(
            prefix=data_mapper_id,
            columns=columns,
            fmt=fmt,
            database=database,
            table=table,
            partition_keys=partition_keys,
            partitions=partitions,
            partition_key_types=partition_key_types,
        )

        items.append(item)
        return item

    yield factory

    empty_table(data_mapper_table, "DataMapperId")


@pytest.fixture(scope="module")
def jobs_endpoint():
    """
    Return a list of the endpoint.

    Args:
    """
    return "jobs"


@pytest.fixture(scope="module")
def job_table(ddb_resource, stack):
    """
    Convert a table of a table

    Args:
        ddb_resource: (todo): write your description
        stack: (list): write your description
    """
    return ddb_resource.Table(stack["JobTable"])


@pytest.fixture(scope="module")
def empty_jobs(job_table):
    """
    Yields.

    Args:
        job_table: (todo): write your description
    """
    empty_table(job_table, "Id", "Sk")
    yield
    empty_table(job_table, "Id", "Sk")


@pytest.fixture
def job_factory(job_table, sf_client, stack):
    """
    Execute a job.

    Args:
        job_table: (todo): write your description
        sf_client: (todo): write your description
        stack: (list): write your description
    """
    items = []

    def factory(
        job_id=str(uuid4()),
        status="QUEUED",
        gsib="0",
        created_at=round(datetime.datetime.now().timestamp()),
        del_queue_items=[],
        **kwargs
    ):
        """
        Factory

        Args:
            job_id: (str): write your description
            str: (todo): write your description
            uuid4: (array): write your description
            status: (str): write your description
            gsib: (array): write your description
            created_at: (bool): write your description
            round: (array): write your description
            datetime: (todo): write your description
            datetime: (todo): write your description
            now: (todo): write your description
            timestamp: (int): write your description
            del_queue_items: (bool): write your description
        """
        item = {
            "Id": job_id,
            "Sk": job_id,
            "Type": "Job",
            "JobStatus": status,
            "CreatedAt": created_at,
            "GSIBucket": gsib,
            "DeletionQueueItems": del_queue_items,
            "DeletionQueueItemsSkipped": False,
            "AthenaConcurrencyLimit": 15,
            "DeletionTasksMaxNumber": 1,
            "QueryExecutionWaitSeconds": 1,
            "QueryQueueWaitSeconds": 1,
            "ForgetQueueWaitSeconds": 5,
            **kwargs,
        }
        job_table.put_item(Item=item)
        items.append(
            "{}:{}".format(
                stack["StateMachineArn"].replace("stateMachine", "execution"), job_id
            )
        )
        return item

    yield factory

    empty_table(job_table, "Id", "Sk")
    for arn in items:
        try:
            sf_client.stop_execution(executionArn=arn)
        except Exception as e:
            logger.warning("Unable to stop execution: {}".format(str(e)))


def get_waiter_model(config_file):
    """
    Get the model from the configuration file.

    Args:
        config_file: (str): write your description
    """
    waiter_dir = Path(__file__).parent.parent.joinpath("waiters")
    with open(waiter_dir.joinpath(config_file)) as f:
        config = json.load(f)
    return WaiterModel(config)


@pytest.fixture(scope="session")
def execution_waiter(sf_client):
    """
    Return a : class.

    Args:
        sf_client: (todo): write your description
    """
    waiter_model = get_waiter_model("stepfunctions.json")
    return create_waiter_with_client("ExecutionComplete", waiter_model, sf_client)


@pytest.fixture(scope="session")
def execution_exists_waiter(sf_client):
    """
    Return true if the given model is ready.

    Args:
        sf_client: (todo): write your description
    """
    waiter_model = get_waiter_model("stepfunctions.json")
    return create_waiter_with_client("ExecutionExists", waiter_model, sf_client)


@pytest.fixture(scope="session")
def job_complete_waiter(ddb_client):
    """
    Create a list of completed jobs.

    Args:
        ddb_client: (todo): write your description
    """
    waiter_model = get_waiter_model("jobs.json")
    return create_waiter_with_client("JobComplete", waiter_model, ddb_client)


@pytest.fixture(scope="session")
def job_finished_waiter(ddb_client):
    """
    Create a list of the waiting for each job.

    Args:
        ddb_client: (todo): write your description
    """
    waiter_model = get_waiter_model("jobs.json")
    return create_waiter_with_client("JobFinished", waiter_model, ddb_client)


@pytest.fixture(scope="session")
def job_exists_waiter(ddb_client):
    """
    Return true if the job is waiting.

    Args:
        ddb_client: (todo): write your description
    """
    waiter_model = get_waiter_model("jobs.json")
    return create_waiter_with_client("JobExists", waiter_model, ddb_client)


@pytest.fixture(scope="module")
def empty_lake(dummy_lake):
    """
    Empty all empty dummy.

    Args:
        dummy_lake: (todo): write your description
    """
    dummy_lake["bucket"].objects.delete()


@pytest.fixture(scope="session")
def dummy_lake(s3_resource, stack, data_access_role):
    """
    Context manager for a bucket.

    Args:
        s3_resource: (todo): write your description
        stack: (list): write your description
        data_access_role: (todo): write your description
    """
    # Lake Config
    bucket_name = "test-" + str(uuid4())
    # Create the bucket and Glue table
    bucket = s3_resource.Bucket(bucket_name)
    policy = s3_resource.BucketPolicy(bucket_name)
    bucket.create(
        CreateBucketConfiguration={
            "LocationConstraint": getenv("AWS_DEFAULT_REGION", "eu-west-1")
        },
    )
    bucket.wait_until_exists()
    s3_resource.BucketVersioning(bucket_name).enable()
    roles = [stack["AthenaExecutionRoleArn"], stack["DeleteTaskRoleArn"]]
    if data_access_role:
        roles.append(data_access_role["Arn"])
    policy.put(
        Policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": roles},
                        "Action": "s3:*",
                        "Resource": [
                            "arn:aws:s3:::{}".format(bucket_name),
                            "arn:aws:s3:::{}/*".format(bucket_name),
                        ],
                    }
                ],
            }
        )
    )

    yield {"bucket_name": bucket_name, "bucket": bucket, "policy": policy}

    # Cleanup
    bucket.objects.delete()
    bucket.object_versions.delete()
    bucket.delete()


@pytest.fixture
def policy_changer(dummy_lake):
    """
    A context manager to a bucket.

    Args:
        dummy_lake: (todo): write your description
    """
    bucket = dummy_lake["bucket"]
    policy = bucket.Policy()
    original = policy.policy

    def update_policy(temp_policy):
        """
        Update the specified policy.

        Args:
            temp_policy: (todo): write your description
        """
        policy.put(Policy=json.dumps(temp_policy))

    yield update_policy
    # reset policy back
    policy.put(Policy=original)


@pytest.fixture
def data_loader(dummy_lake):
    """
    Yield generator that object.

    Args:
        dummy_lake: (todo): write your description
    """
    loaded_data = []
    bucket = dummy_lake["bucket"]

    def load_data(filename, object_key, **kwargs):
        """
        Load data from a file.

        Args:
            filename: (str): write your description
            object_key: (str): write your description
        """
        file_path = str(Path(__file__).parent.joinpath("data").joinpath(filename))
        bucket.upload_file(file_path, object_key, ExtraArgs=kwargs)
        loaded_data.append(object_key)

    yield load_data

    for d in loaded_data:
        bucket.objects.filter(Prefix=d).delete()
        bucket.object_versions.filter(Prefix=d).delete()


def fetch_total_messages(q):
    """
    Returns total number of total total number of total number of total total number of total total total number of total number of total number of total total number of

    Args:
        q: (todo): write your description
    """
    return int(q.attributes["ApproximateNumberOfMessages"]) + int(
        q.attributes["ApproximateNumberOfMessagesNotVisible"]
    )


@pytest.fixture(scope="session")
def query_queue(stack):
    """
    Query the queue for a queue

    Args:
        stack: (list): write your description
    """
    queue = boto3.resource("sqs").Queue(stack["QueryQueueUrl"])
    if fetch_total_messages(queue) > 0:
        queue.purge()
    return queue


@pytest.fixture(scope="session")
def fargate_queue(stack):
    """
    Fetches a queue from a queue.

    Args:
        stack: (list): write your description
    """
    queue = boto3.resource("sqs").Queue(stack["DeletionQueueUrl"])
    if fetch_total_messages(queue) > 0:
        queue.purge()
    return queue


@pytest.fixture
def queue_reader(sf_client):
    """
    Queue a message from the queue.

    Args:
        sf_client: (todo): write your description
    """
    def read(queue, msgs_to_read=10):
        """
        Read messages from the queue.

        Args:
            queue: (todo): write your description
            msgs_to_read: (str): write your description
        """
        messages = queue.receive_messages(
            WaitTimeSeconds=5, MaxNumberOfMessages=msgs_to_read
        )
        for message in messages:
            message.delete()
            body = json.loads(message.body)
            if body.get("TaskToken"):
                sf_client.send_task_success(
                    taskToken=body["TaskToken"], output=json.dumps({})
                )

        return messages

    return read


@pytest.fixture(scope="session")
def data_access_role(iam_client):
    """
    Get iam access role

    Args:
        iam_client: (todo): write your description
    """
    try:
        return iam_client.get_role(RoleName="S3F2DataAccessRole")["Role"]
    except ClientError as e:
        logger.warning(str(e))
        pytest.exit("Abandoning test run due to missing data access role", 1)
