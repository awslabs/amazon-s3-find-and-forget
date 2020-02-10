import os
from types import SimpleNamespace

import pytest
import boto3
from mock import patch, Mock


with patch.dict(os.environ, {"JobTable": "test", "DeletionQueueTable": "test"}):
    from backend.lambdas.jobs.stream_processor import handler, is_operation, is_job, is_job_event, process_job, \
        clear_deletion_queue

pytestmark = [pytest.mark.unit, pytest.mark.jobs]


def test_it_processes_matches_events():
    assert is_operation({
        "eventName": "INSERT"
    }, "INSERT")
    assert is_operation({
        "eventName": "MODIFY"
    }, "MODIFY")
    

def test_it_recognises_jobs():
    assert is_job({
        "dynamodb": {
            "NewImage": {
                "Id": {"S": "job123"},
                "Sk": {"S": "job123"},
                "Type": {"S": "Job"},
            }
        }
    })
    assert not is_job({
        "dynamodb": {
            "NewImage": {
                "Id": {"S": "job123"},
                "Sk": {"S": "123456"},
                "Type": {"S": "JobEvent"},
            }
        }
    })

    
def test_it_recognises_job_events():
    assert is_job_event({
        "dynamodb": {
            "NewImage": {
                "Id": {"S": "job123"},
                "Sk": {"S": "123456"},
                "Type": {"S": "JobEvent"},
            }
        }
    })
    assert not is_job_event({
        "dynamodb": {
            "NewImage": {
                "Id": {"S": "job123"},
                "Sk": {"S": "123456"},
                "Type": {"S": "Job"},
            }
        }
    })


@patch("backend.lambdas.jobs.stream_processor.is_job", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.is_job_event", Mock(return_value=False))
@patch("backend.lambdas.jobs.stream_processor.process_job")
@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_handles_job_records(mock_deserializer, mock_process):
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "job123",
        "Type": "Job",
    }
    handler({
        "Records": [{
            "eventName": "INSERT",
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "job123"},
                    "Type": {"S": "Job"},
                }
            }
        }]
    }, SimpleNamespace())

    assert 1 == mock_process.call_count
    assert 2 == mock_deserializer.call_count


@patch("backend.lambdas.jobs.stream_processor.is_job", Mock(return_value=False))
@patch("backend.lambdas.jobs.stream_processor.is_job_event", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.update_status")
@patch("backend.lambdas.jobs.stream_processor.update_stats")
@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_handles_job_event_records(mock_deserializer, mock_stats, mock_status):
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
    }
    mock_status.return_value = "RUNNING"
    mock_stats.return_value = {}

    handler({
        "Records": [{
            "eventName": "INSERT",
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "123456"},
                    "Type": {"S": "JobEvent"},
                }
            }
        }]
    }, SimpleNamespace())

    assert 1 == mock_status.call_count
    assert 1 == mock_stats.call_count
    assert 1 == mock_deserializer.call_count


@patch("backend.lambdas.jobs.stream_processor.is_job", Mock(return_value=False))
@patch("backend.lambdas.jobs.stream_processor.is_job_event", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.update_status")
@patch("backend.lambdas.jobs.stream_processor.update_stats")
@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_does_not_update_status_if_stats_fails(mock_deserializer, mock_stats, mock_status):
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
    }
    mock_stats.side_effect = ValueError

    with pytest.raises(ValueError):
        handler({
            "Records": [{
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {
                        "Id": {"S": "job123"},
                        "Sk": {"S": "123456"},
                        "Type": {"S": "JobEvent"},
                    }
                }
            }]
        }, SimpleNamespace())

    mock_status.assert_not_called()


@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.client")
def test_it_starts_state_machine(mock_client):
    process_job({
        "Id": "job123",
        "Sk": "job123",
        "Type": "Job",
        "AthenaConcurrencyLimit": 15,
        "DeletionTasksMaxNumber": 50,
        "WaitDurationQueryExecution": 5,
        "WaitDurationQueryQueue": 5,
        "WaitDurationForgetQueue": 30
    })

    mock_client.start_execution.assert_called()


@patch("backend.lambdas.jobs.stream_processor.is_job", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.is_job_event", Mock(return_value=False))
@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.client")
def test_it_handles_already_existing_executions(mock_client):
    e = boto3.client("stepfunctions").exceptions.ExecutionAlreadyExists
    mock_client.exceptions.ExecutionAlreadyExists = e
    mock_client.start_execution.side_effect = e({}, "ExecutionAlreadyExists")
    process_job({
        "Id": "job123",
        "Sk": "job123",
        "Type": "Job",
        "CreatedAt": 123.0,
    })


@patch("backend.lambdas.jobs.stream_processor.is_job", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.is_job_event", Mock(return_value=False))
@patch("backend.lambdas.jobs.stream_processor.process_job", Mock(return_value=None))
@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
@patch("backend.lambdas.jobs.stream_processor.clear_deletion_queue")
def test_it_handles_job_records(mock_clear_queue, mock_deserializer):
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "job123",
        "Type": "Job",
        "JobStatus": "COMPLETED",
    }
    handler({
        "Records": [{
            "eventName": "MODIFY",
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "job123"},
                    "Type": {"S": "Job"},
                    "JobStatus": {"S": "COMPLETED"}
                }
            }
        }]
    }, SimpleNamespace())

    mock_clear_queue.assert_called()


@patch("backend.lambdas.jobs.stream_processor.q_table.batch_writer")
def test_it_clears_queue(mock_writer):
    mock_writer.return_value.__enter__.return_value = mock_writer
    clear_deletion_queue({
        "Id": "job123",
        "Sk": "job123",
        "Type": "Job",
        "JobStatus": "COMPLETED",
        "DeletionQueue": [
            {"MatchId": "test", "CreatedAt": 123456789}
        ]
    })

    mock_writer.delete_item.assert_called()
