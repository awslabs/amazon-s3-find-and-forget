from types import SimpleNamespace

import pytest
import boto3
from mock import patch, Mock

from backend.lambdas.queue.stream_processor import handler, should_process

pytestmark = [pytest.mark.unit, pytest.mark.queue]


def test_it_skips_non_inserts():
    assert not should_process({
        "eventName": "UPDATE"
    })


def test_it_processes_inserts():
    assert should_process({
        "eventName": "INSERT"
    })


@patch("backend.lambdas.queue.stream_processor.should_process", Mock(return_value=True))
@patch("backend.lambdas.queue.stream_processor.deserializer")
@patch("backend.lambdas.queue.stream_processor.client")
def test_it_starts_state_machine(mock_client, mock_deserializer):
    mock_deserializer.deserialize.return_value = "val"
    handler({
        "Records": [{
            "eventName": "INSERT",
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "job123"},
                }
            }
        }]
    }, SimpleNamespace())

    mock_client.start_execution.assert_called()


@patch("backend.lambdas.queue.stream_processor.should_process", Mock(return_value=True))
@patch("backend.lambdas.queue.stream_processor.deserializer")
@patch("backend.lambdas.queue.stream_processor.client")
def test_it_decodes_decimals(mock_client, mock_deserializer):
    mock_deserializer.deserialize.return_value = "val"
    handler({
        "Records": [{
            "eventName": "INSERT",
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "job123"},
                    "CreatedAt": {"N": 123.0},
                }
            }
        }]
    }, SimpleNamespace())

    mock_client.start_execution.assert_called()


@patch("backend.lambdas.queue.stream_processor.should_process", Mock(return_value=True))
@patch("backend.lambdas.queue.stream_processor.deserializer")
@patch("backend.lambdas.queue.stream_processor.client")
def test_it_decodes_decimals(mock_client, mock_deserializer):
    mock_deserializer.deserialize.return_value = "val"
    handler({
        "Records": [{
            "eventName": "INSERT",
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "job123"},
                    "CreatedAt": {"N": 123.0},
                }
            }
        }]
    }, SimpleNamespace())

    mock_client.start_execution.assert_called()


@patch("backend.lambdas.queue.stream_processor.should_process", Mock(return_value=True))
@patch("backend.lambdas.queue.stream_processor.deserializer")
@patch("backend.lambdas.queue.stream_processor.client")
def test_it_handles_already_existing_executions(mock_client, mock_deserializer):
    e = boto3.client("stepfunctions").exceptions.ExecutionAlreadyExists
    mock_deserializer.deserialize.return_value = "val"
    mock_client.exceptions.ExecutionAlreadyExists = e
    mock_client.start_execution.side_effect = e({}, "ExecutionAlreadyExists")
    handler({
        "Records": [{
            "eventName": "INSERT",
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "job123"},
                    "CreatedAt": {"N": 123.0},
                }
            }
        }]
    }, SimpleNamespace())
