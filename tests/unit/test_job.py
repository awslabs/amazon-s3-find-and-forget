import json
import os
from types import SimpleNamespace

import pytest
from mock import patch

from lambdas.src.jobs import handlers

pytestmark = [pytest.mark.unit, pytest.mark.jobs]


@patch("lambdas.src.jobs.handlers.sf_client")
@patch("lambdas.src.jobs.handlers.get_execution_arn")
def test_it_retrieves_all_items(get_execution_arn, sf_client):
    mock_exec_arn = "arn:aws:states:eu-west-1:123456789012:execution:HelloWorld:test"
    sf_client.describe_execution.return_value = {
        "name": "test",
        "executionArn": mock_exec_arn,
        "status": "RUNNING"
    }
    get_execution_arn.return_value = mock_exec_arn
    response = handlers.get_job_handler({
        "pathParameters": {"job_id": "test"}
    }, SimpleNamespace())
    assert {
        "statusCode": 200,
        "body": json.dumps({
            "JobId": "test",
            "Status": "RUNNING"
        })
    } == response


def test_it_derives_execution_arn():
    expected_arn = "arn:aws:states:eu-west-1:123456789012:execution:ContainsstateMachine:test"
    mock_env = {"StateMachineArn": "arn:aws:states:eu-west-1:123456789012:stateMachine:ContainsstateMachine"}
    with patch.dict(os.environ, mock_env):
        actual = handlers.get_execution_arn("test")
        assert expected_arn == actual
