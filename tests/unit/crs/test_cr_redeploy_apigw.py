import pytest
from mock import patch, MagicMock

from backend.lambdas.custom_resources.redeploy_apigw import create, update, handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.redeploy_apigw.api_client")
def test_it_triggers_deployment_if_required(mock_client):
    event = {
        "ResourceProperties": {
            "DeployCognito": "true",
            "ApiId": "abcdefghij",
            "ApiStage": "prod",
        },
        "OldResourceProperties": {"DeployCognito": "false"},
    }

    resp = update(event, MagicMock())

    mock_client.create_deployment.assert_called_with(
        restApiId="abcdefghij", stageName="prod"
    )

    assert not resp


@patch("backend.lambdas.custom_resources.redeploy_apigw.api_client")
def test_it_does_not_trigger_deployment_if_not_required(mock_client):
    event = {
        "ResourceProperties": {"DeployCognito": "true"},
        "OldResourceProperties": {"DeployCognito": "true"},
    }

    resp = update(event, MagicMock())

    mock_client.assert_not_called()

    assert not resp


@patch("backend.lambdas.custom_resources.redeploy_apigw.api_client")
def test_it_does_nothing_on_create(mock_client):
    resp = create({}, MagicMock())

    mock_client.assert_not_called()
    assert not resp


@patch("backend.lambdas.custom_resources.redeploy_apigw.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)
