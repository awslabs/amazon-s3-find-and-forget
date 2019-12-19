import datetime
import json
from types import SimpleNamespace

import pytest
from mock import patch, ANY

from backend.lambdas.jobs import handlers

pytestmark = [pytest.mark.unit, pytest.mark.api, pytest.mark.jobs]


@patch("backend.lambdas.jobs.handlers.sf_client")
def test_it_retrieves_in_flight_jobs(sf_client):
    mock_exec_arn = "arn:aws:states:eu-west-1:123456789012:execution:HelloWorld:test"
    sf_client.list_executions.return_value = {
        "executions": [{
            "name": "test",
            "executionArn": mock_exec_arn,
            "status": "RUNNING",
            "startDate": datetime.datetime.now()
        }, {
            "name": "other",
            "executionArn": mock_exec_arn,
            "status": "RUNNING",
            "startDate": datetime.datetime.now()
        }]
    }
    response = handlers.get_job_handler({
        "pathParameters": {"job_id": "test"}
    }, SimpleNamespace())

    assert 200 == response["statusCode"]
    assert {
        "JobId": "test",
        "JobStatus": "RUNNING",
        "StartTime": ANY
    } == json.loads(response["body"])
    assert ANY == response["headers"]


@patch("backend.lambdas.jobs.handlers.get_object_contents")
@patch("backend.lambdas.jobs.handlers.sf_client")
def test_it_retrieves_in_summary_for_completed(sf_client, get_object_contents):
    sf_client.list_executions.return_value = {
        "executions": []
    }
    get_object_contents.return_value = json.dumps({
        "JobId": "test",
        "JobStatus": "COMPLETED",
        "StartTime": datetime.datetime.now().isoformat().replace('+00:00', 'Z'),
        "EndTime": datetime.datetime.now().isoformat().replace('+00:00', 'Z')
    })
    response = handlers.get_job_handler({
        "pathParameters": {"job_id": "test"}
    }, SimpleNamespace())

    assert 200 == response["statusCode"]
    assert {
        "JobId": "test",
        "JobStatus": "COMPLETED",
        "StartTime": ANY,
        "EndTime": ANY
    } == json.loads(response["body"])
    assert ANY == response["headers"]

