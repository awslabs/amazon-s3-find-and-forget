import json
from types import SimpleNamespace

import pytest

from backend.lambdas.tasks.work_query_queue import handler, any_query_has_failed
from mock import patch, ANY, MagicMock

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
@patch("backend.lambdas.tasks.work_query_queue.any_query_has_failed", MagicMock(return_value=False))
def test_it_skips_with_no_remaining_capacity(sqs_mock, read_queue_mock):
    sqs_mock.Queue.return_value = sqs_mock

    handler({
        "ExecutionId": "1234",
        "ExecutionName": "4231",
        "QueryQueue": {
            "NotVisible": 20,
            "Visible": 50,
        }
    }, SimpleNamespace())

    read_queue_mock.assert_not_called()


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
@patch("backend.lambdas.tasks.work_query_queue.any_query_has_failed", MagicMock(return_value=False))
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
        "AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID": "1234",
        "JobId": "4231",
        "ReceiptHandle": "1234",
        "WaitDuration": 15
    })

    handler({
        "ExecutionId": "1234",
        "ExecutionName": "4231",
        "QueryQueue": {
            "NotVisible": 0,
            "Visible": 50,
        }
    }, SimpleNamespace())

    sf_client_mock.start_execution.assert_called_with(stateMachineArn=ANY, input=expected_call)


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
@patch("backend.lambdas.tasks.work_query_queue.any_query_has_failed", MagicMock(return_value=False))
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
        "ExecutionId": "1234",
        "ExecutionName": "4231",
        "QueryQueue": {
            "NotVisible": 0,
            "Visible": 50,
        }
    }, SimpleNamespace())

    assert 2 == sf_client_mock.start_execution.call_count


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
@patch("backend.lambdas.tasks.work_query_queue.any_query_has_failed", MagicMock(return_value=False))
def test_limits_calls_to_capacity(sqs_mock, read_queue_mock, sf_client_mock):
    sqs_mock.Queue.return_value = sqs_mock
    read_queue_mock.return_value = [
        SimpleNamespace(
            body=json.dumps({"hello": "world"}),
            receipt_handle="1234",
        ),
    ]

    handler({
        "ExecutionId": "1234",
        "ExecutionName": "4231",
        "QueryQueue": {
            "NotVisible": 19,
            "Visible": 50,
        }
    }, SimpleNamespace())

    read_queue_mock.assert_called_with(ANY, 1)
    assert 1 == sf_client_mock.start_execution.call_count


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
@patch("backend.lambdas.tasks.work_query_queue.any_query_has_failed", MagicMock(return_value=False))
def test_it_only_requests_the_minimum(sqs_mock, read_queue_mock, sf_client_mock):
    sqs_mock.Queue.return_value = sqs_mock
    read_queue_mock.return_value = [
        SimpleNamespace(
            body=json.dumps({"hello": "world"}),
            receipt_handle="1234",
        ),
    ]

    handler({
        "ExecutionId": "1234",
        "ExecutionName": "4321",
        "QueryQueue": {
            "NotVisible": 0,
            "Visible": 2,
        }
    }, SimpleNamespace())

    read_queue_mock.assert_called_with(ANY, 2)
    assert 1 == sf_client_mock.start_execution.call_count


@patch("backend.lambdas.tasks.work_query_queue.any_query_has_failed", MagicMock(return_value=True))
def test_it_throws_when_any_query_fails():
    with pytest.raises(RuntimeError):
        handler({
            "ExecutionId": "1234",
            "ExecutionName": "4321",
            "QueryQueue": {
                "NotVisible": 0,
                "Visible": 2,
            }
        }, SimpleNamespace())


@patch("backend.lambdas.tasks.work_query_queue.logs")
def test_it_returns_true_if_any_logs_found(mock_logs):
    mock_logs.filter_log_events.return_value = {
        'events': [{'message': 'blah'}]
    }
    resp = any_query_has_failed("123")
    assert resp


@patch("backend.lambdas.tasks.work_query_queue.logs")
def test_it_returns_false_if_no_logs_found(mock_logs):
    mock_logs.filter_log_events.return_value = {
        'events': []
    }
    resp = any_query_has_failed("123")
    assert not resp
