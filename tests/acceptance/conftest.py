"""
PyTest Setup and Fixtures
1. Tests not marked as requiring AWS should run against both AWS and local with no changes to the test logic at all
2. Tests marked as requiring AWS should fail when running local resources due to things in AWS not being found
3. The local stack or AWS stack setup should be done outside the test runner as pre-requisite for running tests
4. All setup for running local should be contained in this file.
"""
import logging
from functools import partial
from os import getenv
from urllib.parse import urljoin

import boto3
import pytest
from aws_xray_sdk import global_sdk_config
from botocore.exceptions import ClientError
from requests import Session

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
    # Setup DDB local resource
    session = boto3.Session(profile_name=getenv("AWS_PROFILE", "default"))

    return session.resource('dynamodb')


@pytest.fixture(scope="session")
def sf_client():
    session = boto3.Session(profile_name=getenv("AWS_PROFILE", "default"))

    return session.client('stepfunctions')


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


@pytest.fixture(scope="module")
def jobs_endpoint():
    return "jobs"


@pytest.fixture
def state_machine(sf_client):
    yield {
        "stateMachineArn": getenv("StateMachineArn")
    }


@pytest.fixture
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
