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
def test_it_process_queue(table, uuid):
    uuid.uuid4.return_value = 123
    response = handlers.process_handler({
        "body": ""
    }, SimpleNamespace())
    table.put_item.assert_called_with(Item={
        "Id": "123",
        "Type": "Job",
        "JobStatus": "QUEUED",
        "GSIBucket": "0",
        "CreatedAt": ANY,
    })
    assert 202 == response["statusCode"]
    assert "headers" in response
    assert {
        "Id": "123",
        "Type": "Job",
        "JobStatus": "QUEUED",
        "GSIBucket": "0",
        "CreatedAt": ANY,
    } == json.loads(response["body"])
