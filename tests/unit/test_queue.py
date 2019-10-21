import json
from types import SimpleNamespace

import pytest
from mock import patch

from lambdas.src.queue import handlers

pytestmark = [pytest.mark.unit, pytest.mark.queue]


@patch("lambdas.src.queue.handlers.table")
def test_it_retrieves_all_items(table):
    table.scan.return_value = {"Items": []}
    response = handlers.get_handler({}, SimpleNamespace())
    assert {
        "statusCode": 200,
        "body": json.dumps({"MatchIds": []})
    } == response


@patch("lambdas.src.queue.handlers.table")
def test_it_add_to_queue(table):
    response = handlers.enqueue_handler({
        "body": json.dumps({
            "MatchId": "test",
            "Columns": ["a"],
        })
    }, SimpleNamespace())

    assert 201 == response["statusCode"]
    assert {
        "MatchId": "test",
        "Columns": ["a"],
    } == json.loads(response["body"])


@patch("lambdas.src.queue.handlers.table")
def test_it_provides_default_columns(table):
    response = handlers.enqueue_handler({
        "body": json.dumps({
            "MatchId": "test"
        })
    }, SimpleNamespace())

    assert 201 == response["statusCode"]
    assert {
        "MatchId": "test",
        "Columns": [],
    } == json.loads(response["body"])


@patch("lambdas.src.queue.handlers.table")
def test_it_cancels_deletions(table):
    response = handlers.cancel_handler({
        "pathParameters": {
            "match_id": "test",
        }
    }, SimpleNamespace())
    assert {
        "statusCode": 204
    } == response
