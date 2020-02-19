import json
import os
from types import SimpleNamespace

import pytest
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
        "CreatedAt": ANY,
        "DataMappers": ["a"],
    } == json.loads(response["body"])


@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_provides_default_data_mappers(table):
    response = handlers.enqueue_handler({
        "body": json.dumps({
            "MatchId": "test",
        })
    }, SimpleNamespace())

    assert 201 == response["statusCode"]
    assert {
        "MatchId": "test",
        "CreatedAt": ANY,
        "DataMappers": [],
    } == json.loads(response["body"])


@patch("backend.lambdas.queue.handlers.running_job_exists")
@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_cancels_deletions(table, mock_running_job):
    mock_running_job.return_value = False
    response = handlers.cancel_handler({
        "body": json.dumps({
            "Matches": [{
                "MatchId": "test",
                "CreatedAt": 123456789,
            }],
        })
    }, SimpleNamespace())
    assert {
        "statusCode": 204,
        "headers": ANY
    } == response


@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_rejects_invalid_delete_requests(table):
    response = handlers.cancel_handler({
        "body": json.dumps({
            "Matches": [{
                "MatchId": "test"
            }],
        })
    }, SimpleNamespace())
    assert {
        "statusCode": 422,
        "headers": ANY,
        "body": ANY
    } == response


@patch("backend.lambdas.queue.handlers.running_job_exists")
def test_it_prevents_cancelling_whilst_running_jobs(mock_running_job):
    mock_running_job.return_value = True
    response = handlers.cancel_handler({
        "body": json.dumps({
            "Matches": [{
                "MatchId": "test",
                "CreatedAt": 123456789,
            }],
        })
    }, SimpleNamespace())

    assert 400 == response["statusCode"]
    assert "headers" in response


@patch("backend.lambdas.queue.handlers.bucket_count", 1)
@patch("backend.lambdas.queue.handlers.uuid")
@patch("backend.lambdas.queue.handlers.deletion_queue_table")
@patch("backend.lambdas.queue.handlers.jobs_table")
@patch("backend.lambdas.queue.handlers.running_job_exists")
@patch("backend.lambdas.queue.handlers.get_config")
def test_it_process_queue(mock_config, mock_running_job, job_table, q_table, uuid):
    mock_running_job.return_value = False
    mock_config.return_value = {
        "AthenaConcurrencyLimit": 15,
        "DeletionTasksMaxNumber": 50,
        "WaitDurationQueryExecution": 5,
        "WaitDurationQueryQueue": 5,
        "WaitDurationForgetQueue": 30
    }
    q_table.scan.return_value = {"Items": [{"MatchId": "123", "CreatedAt": 123}]}
    uuid.uuid4.return_value = 123
    response = handlers.process_handler({
        "body": ""
    }, SimpleNamespace())
    job_table.put_item.assert_called_with(Item={
        "Id": "123",
        "Sk": "123",
        "Type": "Job",
        "JobStatus": "QUEUED",
        "GSIBucket": "0",
        "CreatedAt": ANY,
        "DeletionQueueItems": [{"MatchId": "123", "CreatedAt": 123}],
        "AthenaConcurrencyLimit": 15,
        "DeletionTasksMaxNumber": 50,
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
        "DeletionQueueItems": [{"MatchId": "123", "CreatedAt": 123}],
        "AthenaConcurrencyLimit": 15,
        "DeletionTasksMaxNumber": 50,
        "WaitDurationQueryExecution": 5,
        "WaitDurationQueryQueue": 5,
        "WaitDurationForgetQueue": 30
    } == json.loads(response["body"])


@patch("backend.lambdas.queue.handlers.running_job_exists")
def test_it_prevents_concurrent_running_jobs(mock_running_job):
    mock_running_job.return_value = True
    response = handlers.process_handler({
        "body": ""
    }, SimpleNamespace())

    assert 400 == response["statusCode"]
    assert "headers" in response
