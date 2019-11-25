import json
from types import SimpleNamespace

import pytest

from backend.lambdas.tasks.work_query_queue import handler
from mock import patch, ANY

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
def test_it_skips_with_no_remaining_capacity(sqs_mock, read_queue_mock):
    sqs_mock.Queue.return_value = sqs_mock

    handler({
        "QueryQueue": {
            "NotVisible": 20
        }
    }, SimpleNamespace())

    read_queue_mock.assert_not_called()


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
def test_it_adds_receipt_handle(sqs_mock, read_queue_mock, sf_client_mock):
    sqs_mock.Queue.return_value = sqs_mock
    read_queue_mock.return_value = [
        SimpleNamespace(
            body=json.dumps({"hello": "world"}),
            receipt_handle="1234",
        )
    ]
    expected_call = json.dumps({
        "hello": "world",
        "ReceiptHandle": "1234",
    })

    handler({
        "QueryQueue": {
            "NotVisible": 0
        }
    }, SimpleNamespace())

    sf_client_mock.start_execution.assert_called_with(stateMachineArn=ANY, input=expected_call)


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
def test_it_starts_state_machine_per_message(sqs_mock, read_queue_mock, sf_client_mock):
    sqs_mock.Queue.return_value = sqs_mock
    read_queue_mock.return_value = [
        SimpleNamespace(
            body=json.dumps({"hello": "world"}),
            receipt_handle="1234",
        ),
        SimpleNamespace(
            body=json.dumps({"other": "world"}),
            receipt_handle="1234",
        )
    ]

    handler({
        "QueryQueue": {
            "NotVisible": 0
        }
    }, SimpleNamespace())

    assert 2 == sf_client_mock.start_execution.call_count


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
def test_limits_calls_to_capacity(sqs_mock, read_queue_mock, sf_client_mock):
    sqs_mock.Queue.return_value = sqs_mock
    read_queue_mock.return_value = [
        SimpleNamespace(
            body=json.dumps({"hello": "world"}),
            receipt_handle="1234",
        ),
    ]

    handler({
        "QueryQueue": {
            "NotVisible": 19
        }
    }, SimpleNamespace())

    read_queue_mock.assert_called_with(ANY, 1)
    assert 1 == sf_client_mock.start_execution.call_count
