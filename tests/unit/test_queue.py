import json
import os
from json import JSONDecodeError
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from mock import patch, ANY

with patch.dict(os.environ, {"DeletionQueueTable": "DeletionQueueTable"}):
    from backend.lambdas.queue import handlers

pytestmark = [pytest.mark.unit, pytest.mark.api, pytest.mark.queue]


@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_retrieves_all_items(table):
    table.scan.return_value = {"Items": []}
    response = handlers.get_handler({}, SimpleNamespace())
    assert {
        "statusCode": 200,
        "body": json.dumps({"MatchIds": []}),
        "headers": ANY
    } == response


@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_add_to_queue(table):
    response = handlers.enqueue_handler({
        "body": json.dumps({
            "MatchId": "test",
            "DataMappers": ["a"],
        })
    }, SimpleNamespace())

    assert 201 == response["statusCode"]
    assert {
        "MatchId": "test",
        "DataMappers": ["a"],
    } == json.loads(response["body"])


@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_provides_default_data_mappers(table):
    response = handlers.enqueue_handler({
        "body": json.dumps({
            "MatchId": "test"
        })
    }, SimpleNamespace())

    assert 201 == response["statusCode"]
    assert {
        "MatchId": "test",
        "DataMappers": [],
    } == json.loads(response["body"])


@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_cancels_deletions(table):
    response = handlers.cancel_handler({
        "body": json.dumps({
            "MatchIds": ["test"],
        })
    }, SimpleNamespace())
    assert {
        "statusCode": 204,
        "headers": ANY
    } == response


@patch("backend.lambdas.queue.handlers.bucket_count", 1)
@patch("backend.lambdas.queue.handlers.uuid")
@patch("backend.lambdas.queue.handlers.jobs_table")
@patch("backend.lambdas.queue.handlers.get_config")
def test_it_process_queue(mock_config, table, uuid):
    mock_config.return_value = {
        "AthenaConcurrencyLimit": 15,
        "DeletionTasksMaxNumber": 50,
        "WaitDurationJobExecution": 120,
        "WaitDurationQueryExecution": 5,
        "WaitDurationQueryQueue": 5,
        "WaitDurationForgetQueue": 30
    }
    uuid.uuid4.return_value = 123
    response = handlers.process_handler({
        "body": ""
    }, SimpleNamespace())
    table.put_item.assert_called_with(Item={
        "Id": "123",
        "Sk": "123",
        "Type": "Job",
        "JobStatus": "QUEUED",
        "GSIBucket": "0",
        "CreatedAt": ANY,
        "AthenaConcurrencyLimit": 15,
        "DeletionTasksMaxNumber": 50,
        "WaitDurationJobExecution": 120,
        "WaitDurationQueryExecution": 5,
        "WaitDurationQueryQueue": 5,
        "WaitDurationForgetQueue": 30
    })
    assert 202 == response["statusCode"]
    assert "headers" in response
    assert {
        "Id": "123",
        "Sk": "123",
        "Type": "Job",
        "JobStatus": "QUEUED",
        "GSIBucket": "0",
        "CreatedAt": ANY,
        "AthenaConcurrencyLimit": 15,
        "DeletionTasksMaxNumber": 50,
        "WaitDurationJobExecution": 120,
        "WaitDurationQueryExecution": 5,
        "WaitDurationQueryQueue": 5,
        "WaitDurationForgetQueue": 30
    } == json.loads(response["body"])


@patch("backend.lambdas.queue.handlers.ssm")
def test_it_retrieves_config(mock_client):
    mock_client.get_parameter.return_value = {
        "Parameter": {
            "Value": json.dumps({
                "AthenaConcurrencyLimit": 1,
                "DeletionTasksMaxNumber": 1,
                "WaitDurationJobExecution": 1,
                "WaitDurationQueryExecution": 1,
                "WaitDurationQueryQueue": 1,
                "WaitDurationForgetQueue": 1
            })
        }
    }
    resp = handlers.get_config()

    assert {
        "AthenaConcurrencyLimit": 1,
        "DeletionTasksMaxNumber": 1,
        "WaitDurationJobExecution": 1,
        "WaitDurationQueryExecution": 1,
        "WaitDurationQueryQueue": 1,
        "WaitDurationForgetQueue": 1
    } == resp


@patch("backend.lambdas.queue.handlers.ssm")
def test_it_handles_invalid_config(mock_client):
    mock_client.get_parameter.return_value = {
        "Parameter": {
            "Value": ""
        }
    }
    with pytest.raises(ValueError):
        handlers.get_config()


@patch("backend.lambdas.queue.handlers.ssm")
def test_it_handles_config_not_found(mock_client):
    mock_client.get_parameter.side_effect = ClientError({}, "get_parameter")
    with pytest.raises(ClientError):
        handlers.get_config()


@patch("backend.lambdas.queue.handlers.ssm")
def test_it_handles_other_config_errors(mock_client):
    mock_client.get_parameter.side_effect = RuntimeError("oops!")
    with pytest.raises(RuntimeError):
        handlers.get_config()
