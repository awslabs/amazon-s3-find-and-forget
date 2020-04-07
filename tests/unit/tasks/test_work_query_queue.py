import json
import os
from types import SimpleNamespace

import pytest
from mock import patch, ANY, MagicMock


with patch.dict(os.environ, {"QueueUrl": "someurl"}):
    from backend.lambdas.tasks.work_query_queue import handler, load_execution, clear_completed, abandon_execution
    # Remove all decorators
    while hasattr(handler, "__wrapped__"):
        handler = handler.__wrapped__

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
@patch("backend.lambdas.tasks.work_query_queue.load_execution")
def test_it_skips_with_no_remaining_capacity(mock_load, sqs_mock, read_queue_mock):
    sqs_mock.Queue.return_value = sqs_mock
    mock_load.return_value = execution_stub(status="RUNNING", ReceiptHandle="handle")

    resp = handler({
        "ExecutionId": "1234",
        "ExecutionName": "4231",
        "RunningExecutions": {
            "Data": list(range(0, 20)),
            "Total": 20
        }
    }, SimpleNamespace())

    read_queue_mock.assert_not_called()
    assert 20 == resp["Total"]
    assert not resp["IsFailing"]


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
def test_it_starts_machine_as_expected(sqs_mock, read_queue_mock, sf_client_mock):
    sqs_mock.Queue.return_value = sqs_mock
    sf_client_mock.start_execution.return_value = execution_stub()

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
        "WaitDuration": 5
    })

    resp = handler({
        "ExecutionId": "1234",
        "ExecutionName": "4231",
        "AthenaConcurrencyLimit": 5,
        "QueryExecutionWaitSeconds": 5,
    }, SimpleNamespace())

    sf_client_mock.start_execution.assert_called_with(stateMachineArn=ANY, input=expected_call)
    assert 1 == resp["Total"]
    assert not resp["IsFailing"]


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
def test_it_defaults_wait_duration(sqs_mock, read_queue_mock, sf_client_mock):
    sqs_mock.Queue.return_value = sqs_mock
    sf_client_mock.start_execution.return_value = execution_stub()

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
        "WaitDuration": 15
    })

    handler({
        "ExecutionId": "1234",
        "ExecutionName": "4231",
    }, SimpleNamespace())

    sf_client_mock.start_execution.assert_called_with(stateMachineArn=ANY, input=expected_call)


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
def test_it_starts_state_machine_per_message(sqs_mock, read_queue_mock, sf_client_mock):
    sqs_mock.Queue.return_value = sqs_mock
    sf_client_mock.start_execution.return_value = execution_stub()
    read_queue_mock.return_value = [
        SimpleNamespace(
            body=json.dumps({"hello": "world"}),
            receipt_handle="1234",
        ),
        SimpleNamespace(
            body=json.dumps({"other": "world"}),
            receipt_handle="4321",
        )
    ]

    resp = handler({
        "ExecutionId": "1234",
        "ExecutionName": "4231",
    }, SimpleNamespace())

    assert 2 == sf_client_mock.start_execution.call_count
    assert 2 == resp["Total"]
    assert not resp["IsFailing"]


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
def test_limits_calls_to_capacity(sqs_mock, read_queue_mock, sf_client_mock):
    sqs_mock.Queue.return_value = sqs_mock
    sf_client_mock.start_execution.return_value = execution_stub()
    read_queue_mock.return_value = [
        SimpleNamespace(
            body=json.dumps({"hello": "world"}),
            receipt_handle="1234",
        ),
    ]

    handler({
        "ExecutionId": "1234",
        "ExecutionName": "4231",
        "AthenaConcurrencyLimit": 20,
    }, SimpleNamespace())

    read_queue_mock.assert_called_with(ANY, 20)


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
@patch("backend.lambdas.tasks.work_query_queue.read_queue")
@patch("backend.lambdas.tasks.work_query_queue.sqs")
@patch("backend.lambdas.tasks.work_query_queue.load_execution")
@patch("backend.lambdas.tasks.work_query_queue.clear_completed")
def test_it_recognises_completed_executions(clear_mock, load_mock, sqs_mock, read_queue_mock, sf_client_mock):
    sqs_mock.Queue.return_value = sqs_mock
    sf_client_mock.start_execution.return_value = execution_stub()
    read_queue_mock.return_value = []
    load_mock.side_effect = [
        *[execution_stub(status="SUCCEEDED") for _ in range(0, 10)],
        *[execution_stub(status="RUNNING", ReceiptHandle="handle") for _ in range(10, 15)],
    ]

    handler({
        "ExecutionId": "1234",
        "ExecutionName": "4231",
        "RunningExecutions": {
            "Data": list(range(0, 15)),
            "Total": 20
        }
    }, SimpleNamespace())

    read_queue_mock.assert_called_with(ANY, 10)


@patch("backend.lambdas.tasks.work_query_queue.load_execution")
@patch("backend.lambdas.tasks.work_query_queue.abandon_execution")
def test_it_abandons_when_any_query_fails_and_no_running_in_current_loop(mock_abandon, mock_load):
    mock_load.return_value = execution_stub(status="FAILED")
    mock_abandon.side_effect = RuntimeError
    with pytest.raises(RuntimeError):
        handler({
            "ExecutionId": "1234",
            "ExecutionName": "4321",
            "RunningExecutions": {
                "IsFailing": True,
                "Data": [{}],
                "Total": 1,
            }
        }, SimpleNamespace())


@patch("backend.lambdas.tasks.work_query_queue.load_execution")
@patch("backend.lambdas.tasks.work_query_queue.abandon_execution")
@patch("backend.lambdas.tasks.work_query_queue.clear_completed")
def test_it_abandons_when_previous_loop_found_failure(mock_clear, mock_abandon, mock_load):
    mock_load.return_value = execution_stub(status="SUCCEEDED", ReceiptHandle="handle")
    mock_abandon.side_effect = RuntimeError
    with pytest.raises(RuntimeError):
        handler({
            "ExecutionId": "1234",
            "ExecutionName": "4321",
            "RunningExecutions": {
                "IsFailing": True,
                "Data": [{}],
                "Total": 1,
            }
        }, SimpleNamespace())


@patch("backend.lambdas.tasks.work_query_queue.load_execution")
@patch("backend.lambdas.tasks.work_query_queue.abandon_execution")
@patch("backend.lambdas.tasks.work_query_queue.sf_client")
def test_it_waits_for_running_executions_before_abandoning(mock_sf, mock_abandon, mock_load):
    mock_load.side_effect = [
        execution_stub(status="FAILED", ReceiptHandle="handle1"),
        execution_stub(status="RUNNING", ReceiptHandle="handle2")
    ]
    res = handler({
        "ExecutionId": "1234",
        "ExecutionName": "4321",
        "RunningExecutions": {
            "IsFailing": False,
            "Data": [{}, {}],
            "Total": 2,
        }
    }, SimpleNamespace())

    mock_abandon.assert_not_called()
    mock_sf.start_execution.assert_not_called()
    assert 1 == res["Total"]
    assert res["IsFailing"]


def test_it_throws_to_abandon():
    with pytest.raises(RuntimeError):
        abandon_execution([execution_stub(status="FAILED")])


@patch("backend.lambdas.tasks.work_query_queue.sf_client")
def test_it_loads_execution_from_state(sf_mock):
    sf_mock.describe_execution.return_value = execution_stub()
    resp = load_execution({
        "ExecutionArn": "arn",
        "ReceiptHandle": "handle"
    })
    assert {
               **execution_stub(),
               "ReceiptHandle": "handle"
           } == resp


@patch("backend.lambdas.tasks.work_query_queue.sqs")
@patch("backend.lambdas.tasks.work_query_queue.queue")
def test_it_clears_completed_from_sqs(mock_queue, mock_sqs):
    mock_message = MagicMock()
    mock_queue.url = "someurl"
    mock_sqs.Message.return_value = mock_message
    clear_completed([
        {"ReceiptHandle": "handle1"},
        {"ReceiptHandle": "handle2"},
    ])
    assert 2 == mock_message.delete.call_count


def execution_stub(**kwargs):
    return {
        "executionArn": "arn:aws:states:eu-west-1:123456789012:execution:S3F2-StateMachine:59923759-3016-82d8-bbc0",
        "stateMachineArn": "arn:aws:states:eu-west-1:123456789012:stateMachine:S3F2-StateMachine",
        "name": "59923759-3016-82d8-bbc0",
        "status": "RUNNING",
        "startDate": 1575900611.248,
        "stopDate": 1575900756.716,
        "input": "{}",
        **kwargs
    }
