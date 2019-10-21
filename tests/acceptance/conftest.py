"""
PyTest Setup and Fixtures
1. Tests not marked as requiring AWS should run against both AWS and local with no changes to the test logic at all
2. Tests marked as requiring AWS should fail when running local resources due to things in AWS not being found
3. The local stack or AWS stack setup should be done outside the test runner as pre-requisite for running tests
4. All setup for running local should be contained in this file.
"""
import json
import logging
from functools import partial
from os import path, getenv
from urllib.parse import urljoin

import boto3
import pytest
from aws_xray_sdk import global_sdk_config
from requests import Session

from . import load_env, DDBLocalManager, get_schema_from_template, load_template, get_resources_from_template

logger = logging.getLogger()

# ENV SETUP
running_local_resources = getenv("RunningLocal", False)


#########
# HOOKS #
#########

def pytest_configure(config):
    """
    Initial test env setup
    """
    global_sdk_config.set_sdk_enabled(False)
    load_env()
    if running_local_resources:
        logger.info("Running in local mode")


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
    # Setup DDB local resource
    kwargs = {}
    if running_local_resources:
        ddb_endpoint = getenv("DdbEndpoint", "http://127.0.0.1:8000")
        session = boto3.Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name=getenv("Region", "eu-west-1"),
        )
        kwargs["endpoint_url"] = ddb_endpoint
    else:
        session = boto3.Session(profile_name=getenv("AWS_PROFILE", "default"))

    return session.resource('dynamodb', **kwargs)


@pytest.fixture(scope="session")
def sf_client():
    # Setup DDB local resource
    kwargs = {}
    if running_local_resources:
        endpoint = getenv("SFEndpoint", "http://127.0.0.1:8083")
        session = boto3.Session(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name=getenv("Region", "eu-west-1"),
        )
        kwargs["endpoint_url"] = endpoint
    else:
        session = boto3.Session(profile_name=getenv("AWS_PROFILE", "default"))

    return session.client('stepfunctions', **kwargs)


@pytest.fixture(scope="session")
def get_table_name():
    def func(prefix, name):
        return "{}_{}".format(prefix, name)

    return partial(func, getenv("TablePrefix"))


@pytest.fixture(scope="session", autouse=running_local_resources)
def local_db(ddb_resource, get_table_name):
    """
    Setups a local DynamoDB database
    """
    if running_local_resources:
        ddb_local_manager = DDBLocalManager(ddb_resource)
        try:
            # Load template
            ddb_template = load_template("ddb.yaml")
            tables = list(get_resources_from_template(ddb_template, "AWS::DynamoDB::Table").keys())
            for table in tables:
                schema = get_schema_from_template(ddb_template, table)
                ddb_local_manager.create_table(get_table_name(table.replace("Table", "")), hash_key=schema["HASH"],
                                               range_key=schema.get("RANGE"))
            yield
        finally:
            ddb_local_manager.delete_tables()


@pytest.fixture(scope="session", autouse=not running_local_resources)
def cognito_token():
    if running_local_resources:
        yield
    else:
        # Generate User in Cognito
        user_pool_id = getenv("UserPoolId")
        client_id = getenv("ClientId")
        username = "aws-uk-sa-builders@amazon.com"
        pwd = "acceptance-tests-password"
        auth_data = {'USERNAME': username, 'PASSWORD': pwd}
        provider_client = boto3.client('cognito-idp')
        # Create the User
        provider_client.admin_create_user(
            UserPoolId=user_pool_id,
            Username=username,
            TemporaryPassword=pwd,
            MessageAction='SUPPRESS',
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
                'ADMIN_NO_SRP_AUTH',
            ],
        )
        # Get JWT token for the dummy user
        resp = provider_client.admin_initiate_auth(UserPoolId=user_pool_id, AuthFlow='ADMIN_NO_SRP_AUTH',
                                                   AuthParameters=auth_data, ClientId=client_id)
        yield resp['AuthenticationResult']['IdToken']
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


@pytest.fixture
def del_queue_item(queue_table, match_id="testId", columns=[]):
    item = {
        "MatchId": match_id,
        "Columns": columns,
    }
    queue_table.put_item(Item=item)
    yield item
    queue_table.delete_item(Key={
        "MatchId": match_id
    })


@pytest.fixture
def event_generator():
    """
    Event generator function. See the events folder
    """

    def load_file(event_type, key_overrides={}):
        events_path = path.join(path.dirname(__file__), 'events')
        file_path = path.join(events_path, event_type + ".json")

        with open(file_path) as f:
            data = json.load(f)
            data.update(key_overrides)

            return data

    return load_file
