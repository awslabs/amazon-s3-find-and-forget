import os
from types import SimpleNamespace

import pytest
import boto3
from botocore.exceptions import ClientError
from mock import patch, Mock, ANY

with patch.dict(os.environ, {"JobTable": "test", "DeletionQueueTable": "test"}):
    from backend.lambdas.jobs.stream_processor import handler, is_operation, is_record_type, process_job, \
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
    assert is_record_type({
        "dynamodb": {
            "NewImage": {
                "Id": {"S": "job123"},
                "Sk": {"S": "job123"},
                "Type": {"S": "Job"},
            }
        }
    }, "Job")
    assert not is_record_type({
        "dynamodb": {
            "NewImage": {
                "Id": {"S": "job123"},
                "Sk": {"S": "123456"},
                "Type": {"S": "JobEvent"},
            }
        }
    }, "Job")
    assert not is_record_type({
        "dynamodb": {}
    }, "Job")

    
def test_it_recognises_job_events():
    assert is_record_type({
        "dynamodb": {
            "NewImage": {
                "Id": {"S": "job123"},
                "Sk": {"S": "123456"},
                "Type": {"S": "JobEvent"},
            }
        }
    }, "JobEvent")
    assert not is_record_type({
        "dynamodb": {
            "NewImage": {
                "Id": {"S": "job123"},
                "Sk": {"S": "123456"},
                "Type": {"S": "Job"},
            }
        }
    }, "JobEvent")


@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.process_job")
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_handles_job_records(mock_deserializer, mock_process, mock_is_record):
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "job123",
        "Type": "Job",
    }
    mock_is_record.side_effect = [True, False]
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
    assert 1 == mock_deserializer.call_count


@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.update_status")
@patch("backend.lambdas.jobs.stream_processor.update_stats")
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_handles_job_event_records(mock_deserializer, mock_stats, mock_status, mock_is_record):
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
    }
    mock_is_record.side_effect = [False, True]
    mock_status.return_value = {"JobStatus": "RUNNING"}
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
    mock_is_record.side_effect = [False, True]

    assert 1 == mock_status.call_count
    assert 1 == mock_stats.call_count
    assert 1 == mock_deserializer.call_count


@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.update_status")
@patch("backend.lambdas.jobs.stream_processor.update_stats")
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_does_not_update_status_if_stats_fails(mock_deserializer, mock_stats, mock_status, mock_is_record):
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
    }
    mock_stats.side_effect = ValueError
    mock_is_record.side_effect = [False, True]

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
        "QueryExecutionWaitSeconds": 5,
        "QueryQueueWaitSeconds": 5,
        "ForgetQueueWaitSeconds": 30
    })

    mock_client.start_execution.assert_called()


@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.client")
def test_it_handles_already_existing_executions(mock_client, mock_is_record):
    e = boto3.client("stepfunctions").exceptions.ExecutionAlreadyExists
    mock_client.exceptions.ExecutionAlreadyExists = e
    mock_client.start_execution.side_effect = e({}, "ExecutionAlreadyExists")
    mock_is_record.side_effect = [True, False]
    process_job({
        "Id": "job123",
        "Sk": "job123",
        "Type": "Job",
        "CreatedAt": 123.0,
    })


@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.client")
@patch("backend.lambdas.jobs.stream_processor.emit_event")
def test_it_handles_execution_failure(mock_emit, mock_client):
    mock_client.start_execution.side_effect = ClientError({}, "start_execution")
    mock_client.exceptions.ExecutionAlreadyExists = boto3.client("stepfunctions").exceptions.ExecutionAlreadyExists
    process_job({
        "Id": "job123",
        "Sk": "job123",
        "Type": "Job",
        "CreatedAt": 123.0,
    })
    mock_emit.assert_called_with("job123", "Exception", {
        "Error": "ExecutionFailure",
        "Cause": "Unable to start StepFunction execution: An error occurred (Unknown) when calling the start_execution operation: Unknown"
    }, "StreamProcessor")

@patch("backend.lambdas.jobs.stream_processor.process_job", Mock(return_value=None))
@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.update_stats", Mock())
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.update_status")
@patch("backend.lambdas.jobs.stream_processor.clear_deletion_queue")
@patch("backend.lambdas.jobs.stream_processor.emit_event")
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_cleans_up_on_forget_complete(mock_deserializer, mock_emit, mock_clear, mock_status, mock_is_record):
    mock_is_record.side_effect = [False, True]
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "event123",
        "Type": "JobEvent",
        "EventName": "ForgetPhaseSucceeded",
    }
    mock_status.return_value = {
        "Id": "job123",
        "Sk": "event123",
        "Type": "Job",
        "JobStatus": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
    }
    handler({
        "Records": [{
            "eventName": "INSERT",
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "job123"},
                    "Type": {"S": "JobEvent"},
                    "EventName": {"S": "ForgetPhaseComplete"}
                }
            }
        }]
    }, SimpleNamespace())

    mock_clear.assert_called()
    mock_emit.assert_called_with(ANY, "CleanupSucceeded", ANY, ANY)


@patch("backend.lambdas.jobs.stream_processor.process_job", Mock(return_value=None))
@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.update_stats", Mock())
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.update_status")
@patch("backend.lambdas.jobs.stream_processor.clear_deletion_queue")
@patch("backend.lambdas.jobs.stream_processor.emit_event")
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_emits_skipped_event_for_failures(mock_deserializer, mock_emit, mock_clear, mock_status, mock_is_record):
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "event123",
        "Type": "JobEvent",
        "EventName": "AnEvent",
    }
    locked_statuses = [
        "FIND_FAILED",
        "FORGET_FAILED",
        "FAILED",
    ]
    mock_status.side_effect = [{
        "Id": "job123",
        "Sk": "event123",
        "Type": "JobEvent",
        "JobStatus": status,
    } for status in locked_statuses]
    mock_is_record.side_effect = list(sum([(False, True) for _ in locked_statuses], ()))
    for _ in locked_statuses:
        handler({
            "Records": [{
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {
                        "Id": {"S": "job123"},
                        "Sk": {"S": "event123"},
                        "Type": {"S": "JobEvent"},
                        "EventName": {"S": "ForgetPhaseEnded"}
                    }
                }
            }]
        }, SimpleNamespace())

        mock_clear.assert_not_called()
        mock_emit.assert_called_with(ANY, "CleanupSkipped", ANY, ANY)


@patch("backend.lambdas.jobs.stream_processor.process_job", Mock(return_value=None))
@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.update_stats", Mock())
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.update_status")
@patch("backend.lambdas.jobs.stream_processor.clear_deletion_queue")
@patch("backend.lambdas.jobs.stream_processor.emit_event")
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_does_not_emit_skipped_event_for_non_failures(
        mock_deserializer, mock_emit, mock_clear, mock_status, mock_is_record
):
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "event123",
        "Type": "JobEvent",
        "EventName": "AnEvent",
    }
    statuses = [
        "RUNNING",
        "QUEUED",
        "COMPLETED",
        "COMPLETED_CLEANUP_FAILED",
    ]
    mock_is_record.side_effect = list(sum([(False, True) for _ in statuses], ()))
    mock_status.side_effect = [{
        "Id": "job123",
        "Sk": "event123",
        "Type": "JobEvent",
        "JobStatus": status,
    } for status in statuses]
    for _ in statuses:
        handler({
            "Records": [{
                "eventName": "INSERT",
                "dynamodb": {
                    "NewImage": {
                        "Id": {"S": "job123"},
                        "Sk": {"S": "event123"},
                        "Type": {"S": "JobEvent"},
                        "EventName": {"S": "ForgetPhaseEnded"}
                    }
                }
            }]
        }, SimpleNamespace())

    for call in mock_emit.call_args_list:
        assert call[0][1] != 'CleanupSkipped'


@patch("backend.lambdas.jobs.stream_processor.process_job", Mock(return_value=None))
@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.update_stats", Mock())
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.update_status")
@patch("backend.lambdas.jobs.stream_processor.clear_deletion_queue")
@patch("backend.lambdas.jobs.stream_processor.emit_event")
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_emits_event_for_cleanup_error(mock_deserializer, mock_emit, mock_clear, mock_status, mock_is_record):
    mock_is_record.side_effect = [False, True]
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "event123",
        "Type": "JobEvent",
        "EventName": "ForgetPhaseSucceeded",
    }
    mock_clear.side_effect = ClientError({}, "delete_item")
    mock_status.return_value = {
        "Id": "job123",
        "Sk": "event123",
        "Type": "JobEvent",
        "JobStatus": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
    }
    handler({
        "Records": [{
            "eventName": "INSERT",
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "job123"},
                    "Type": {"S": "Job"},
                    "EventName": {"S": "ForgetPhaseComplete"}
                }
            }
        }]
    }, SimpleNamespace())

    mock_clear.assert_called()
    mock_emit.assert_called_with(ANY, "CleanupFailed", ANY, ANY)


@patch("backend.lambdas.jobs.stream_processor.q_table.batch_writer")
def test_it_clears_queue(mock_writer):
    mock_writer.return_value.__enter__.return_value = mock_writer
    clear_deletion_queue({
        "Id": "job123",
        "Sk": "job123",
        "Type": "Job",
        "JobStatus": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
        "DeletionQueueItems": [
            {"MatchId": "test", "CreatedAt": 123456789}
        ]
    })

    mock_writer.delete_item.assert_called()
