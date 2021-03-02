import json
import os
from types import SimpleNamespace

import pytest
import boto3
from botocore.exceptions import ClientError
from mock import patch, Mock, ANY, MagicMock, call

with patch.dict(
    os.environ,
    {"JobTable": "test", "DeletionQueueTable": "test", "StateMachineArn": "sm-arn"},
):
    from backend.lambdas.jobs.stream_processor import (
        cleanup_manifests,
        clear_deletion_queue,
        handler,
        is_operation,
        is_record_type,
        process_job,
    )

pytestmark = [pytest.mark.unit, pytest.mark.jobs]


def test_it_processes_matches_events():
    assert is_operation({"eventName": "INSERT"}, "INSERT")
    assert is_operation({"eventName": "MODIFY"}, "MODIFY")


def test_it_recognises_jobs():
    assert is_record_type(
        {
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "job123"},
                    "Type": {"S": "Job"},
                }
            }
        },
        "Job",
        True,
    )
    assert not is_record_type(
        {
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "123456"},
                    "Type": {"S": "JobEvent"},
                }
            }
        },
        "Job",
        True,
    )
    assert not is_record_type({"dynamodb": {}}, "Job", True)


def test_it_recognises_job_events():
    assert is_record_type(
        {
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "123456"},
                    "Type": {"S": "JobEvent"},
                }
            }
        },
        "JobEvent",
        True,
    )
    assert not is_record_type(
        {
            "dynamodb": {
                "NewImage": {
                    "Id": {"S": "job123"},
                    "Sk": {"S": "123456"},
                    "Type": {"S": "Job"},
                }
            }
        },
        "JobEvent",
        True,
    )


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
    mock_is_record.side_effect = [True, False, False]
    handler(
        {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": {
                            "Id": {"S": "job123"},
                            "Sk": {"S": "job123"},
                            "Type": {"S": "Job"},
                        }
                    },
                }
            ]
        },
        SimpleNamespace(),
    )

    assert 1 == mock_process.call_count
    assert 1 == mock_deserializer.call_count


@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.cleanup_manifests")
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_handles_job_deletions(mock_deserializer, mock_cleanup, mock_is_record):
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "job123",
        "Type": "Job",
    }
    mock_is_record.side_effect = [False, True, False]
    handler(
        {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "dynamodb": {
                        "OldImage": {
                            "Id": {"S": "job123"},
                            "Sk": {"S": "job123"},
                            "Type": {"S": "Job"},
                        }
                    },
                }
            ]
        },
        SimpleNamespace(),
    )

    assert 1 == mock_cleanup.call_count
    assert 1 == mock_deserializer.call_count


@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.update_status")
@patch("backend.lambdas.jobs.stream_processor.update_stats")
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_handles_job_event_records(
    mock_deserializer, mock_stats, mock_status, mock_is_record
):
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
    }
    mock_is_record.side_effect = [False, False, True]
    mock_status.return_value = {"JobStatus": "RUNNING"}
    mock_stats.return_value = {}

    handler(
        {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": {
                            "Id": {"S": "job123"},
                            "Sk": {"S": "123456"},
                            "Type": {"S": "JobEvent"},
                        }
                    },
                }
            ]
        },
        SimpleNamespace(),
    )
    mock_is_record.side_effect = [False, False, True]

    assert 1 == mock_status.call_count
    assert 1 == mock_stats.call_count
    assert 1 == mock_deserializer.call_count


@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.update_status")
@patch("backend.lambdas.jobs.stream_processor.update_stats")
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_does_not_update_status_if_stats_fails(
    mock_deserializer, mock_stats, mock_status, mock_is_record
):
    mock_deserializer.return_value = {
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
    }
    mock_stats.side_effect = ValueError
    mock_is_record.side_effect = [False, False, True]

    with pytest.raises(ValueError):
        handler(
            {
                "Records": [
                    {
                        "eventName": "INSERT",
                        "dynamodb": {
                            "NewImage": {
                                "Id": {"S": "job123"},
                                "Sk": {"S": "123456"},
                                "Type": {"S": "JobEvent"},
                            }
                        },
                    }
                ]
            },
            SimpleNamespace(),
        )

    mock_status.assert_not_called()


@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.client")
def test_it_starts_state_machine(mock_client):
    process_job(
        {
            "Id": "job123",
            "Sk": "job123",
            "Type": "Job",
            "AthenaConcurrencyLimit": 15,
            "DeletionTasksMaxNumber": 50,
            "QueryExecutionWaitSeconds": 5,
            "QueryQueueWaitSeconds": 5,
            "ForgetQueueWaitSeconds": 30,
        }
    )

    mock_client.start_execution.assert_called_with(
        stateMachineArn="sm-arn",
        name="job123",
        input=json.dumps(
            {
                "AthenaConcurrencyLimit": 15,
                "DeletionTasksMaxNumber": 50,
                "ForgetQueueWaitSeconds": 30,
                "Id": "job123",
                "QueryExecutionWaitSeconds": 5,
                "QueryQueueWaitSeconds": 5,
            }
        ),
    )


@patch("backend.lambdas.jobs.stream_processor.glue")
@patch("backend.lambdas.jobs.stream_processor.s3")
def test_it_removes_manifests_and_partitions(s3_mock, glue_mock):
    job = {
        "Id": "job-id",
        "Manifests": [
            "s3://bucket/manifests/job-id/dm-1/manifest.json",
            "s3://bucket/manifests/job-id/dm-2/manifest.json",
        ],
    }
    cleanup_manifests(job)
    s3_mock.delete_object.assert_has_calls(
        [
            call(Bucket="bucket", Key="manifests/job-id/dm-1/manifest.json"),
            call(Bucket="bucket", Key="manifests/job-id/dm-2/manifest.json"),
        ]
    )
    glue_mock.batch_delete_partition.assert_called_with(
        DatabaseName="s3f2_manifests_database",
        TableName="s3f2_manifests_table",
        PartitionsToDelete=[
            {"Values": ["job-id", "dm-1"]},
            {"Values": ["job-id", "dm-2"]},
        ],
    )


@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.client")
def test_it_handles_already_existing_executions(mock_client, mock_is_record):
    e = boto3.client("stepfunctions").exceptions.ExecutionAlreadyExists
    mock_client.exceptions.ExecutionAlreadyExists = e
    mock_client.start_execution.side_effect = e({}, "ExecutionAlreadyExists")
    mock_is_record.side_effect = [True, False, False]
    process_job(
        {
            "Id": "job123",
            "Sk": "job123",
            "Type": "Job",
            "CreatedAt": 123.0,
            "AthenaConcurrencyLimit": 15,
            "DeletionTasksMaxNumber": 3,
            "ForgetQueueWaitSeconds": 30,
            "QueryExecutionWaitSeconds": 5,
            "QueryQueueWaitSeconds": 30,
        }
    )


@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.client")
@patch("backend.lambdas.jobs.stream_processor.emit_event")
def test_it_handles_execution_failure(mock_emit, mock_client):
    mock_client.start_execution.side_effect = ClientError({}, "start_execution")
    mock_client.exceptions.ExecutionAlreadyExists = boto3.client(
        "stepfunctions"
    ).exceptions.ExecutionAlreadyExists
    process_job(
        {
            "Id": "job123",
            "Sk": "job123",
            "Type": "Job",
            "CreatedAt": 123.0,
            "AthenaConcurrencyLimit": 15,
            "DeletionTasksMaxNumber": 3,
            "ForgetQueueWaitSeconds": 30,
            "QueryExecutionWaitSeconds": 5,
            "QueryQueueWaitSeconds": 30,
        }
    )
    mock_emit.assert_called_with(
        "job123",
        "Exception",
        {
            "Error": "ExecutionFailure",
            "Cause": "Unable to start StepFunction execution: An error occurred (Unknown) when calling the start_execution operation: Unknown",
        },
        "StreamProcessor",
    )


@patch("backend.lambdas.jobs.stream_processor.process_job", Mock(return_value=None))
@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.update_stats", Mock())
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.update_status")
@patch("backend.lambdas.jobs.stream_processor.clear_deletion_queue")
@patch("backend.lambdas.jobs.stream_processor.emit_event")
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_cleans_up_on_forget_complete(
    mock_deserializer, mock_emit, mock_clear, mock_status, mock_is_record
):
    mock_is_record.side_effect = [False, False, True]
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
    handler(
        {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": {
                            "Id": {"S": "job123"},
                            "Sk": {"S": "job123"},
                            "Type": {"S": "JobEvent"},
                            "EventName": {"S": "ForgetPhaseComplete"},
                        }
                    },
                }
            ]
        },
        SimpleNamespace(),
    )

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
def test_it_emits_skipped_event_for_failures(
    mock_deserializer, mock_emit, mock_clear, mock_status, mock_is_record
):
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
    mock_status.side_effect = [
        {"Id": "job123", "Sk": "event123", "Type": "JobEvent", "JobStatus": status,}
        for status in locked_statuses
    ]
    mock_is_record.side_effect = list(
        sum([(False, False, True) for _ in locked_statuses], ())
    )
    for _ in locked_statuses:
        handler(
            {
                "Records": [
                    {
                        "eventName": "INSERT",
                        "dynamodb": {
                            "NewImage": {
                                "Id": {"S": "job123"},
                                "Sk": {"S": "event123"},
                                "Type": {"S": "JobEvent"},
                                "EventName": {"S": "ForgetPhaseEnded"},
                            }
                        },
                    }
                ]
            },
            SimpleNamespace(),
        )

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
    mock_is_record.side_effect = list(sum([(False, False, True) for _ in statuses], ()))
    mock_status.side_effect = [
        {"Id": "job123", "Sk": "event123", "Type": "JobEvent", "JobStatus": status,}
        for status in statuses
    ]
    for _ in statuses:
        handler(
            {
                "Records": [
                    {
                        "eventName": "INSERT",
                        "dynamodb": {
                            "NewImage": {
                                "Id": {"S": "job123"},
                                "Sk": {"S": "event123"},
                                "Type": {"S": "JobEvent"},
                                "EventName": {"S": "ForgetPhaseEnded"},
                            }
                        },
                    }
                ]
            },
            SimpleNamespace(),
        )

    for call in mock_emit.call_args_list:
        assert call[0][1] != "CleanupSkipped"


@patch("backend.lambdas.jobs.stream_processor.process_job", Mock(return_value=None))
@patch("backend.lambdas.jobs.stream_processor.is_operation", Mock(return_value=True))
@patch("backend.lambdas.jobs.stream_processor.update_stats", Mock())
@patch("backend.lambdas.jobs.stream_processor.is_record_type")
@patch("backend.lambdas.jobs.stream_processor.update_status")
@patch("backend.lambdas.jobs.stream_processor.clear_deletion_queue")
@patch("backend.lambdas.jobs.stream_processor.emit_event")
@patch("backend.lambdas.jobs.stream_processor.deserialize_item")
def test_it_emits_event_for_cleanup_error(
    mock_deserializer, mock_emit, mock_clear, mock_status, mock_is_record
):
    mock_is_record.side_effect = [False, False, True]
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
    handler(
        {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "NewImage": {
                            "Id": {"S": "job123"},
                            "Sk": {"S": "job123"},
                            "Type": {"S": "Job"},
                            "EventName": {"S": "ForgetPhaseComplete"},
                        }
                    },
                }
            ]
        },
        SimpleNamespace(),
    )

    mock_clear.assert_called()
    mock_emit.assert_called_with(ANY, "CleanupFailed", ANY, ANY)


@patch("backend.lambdas.jobs.stream_processor.q_table.batch_writer")
@patch("backend.lambdas.jobs.stream_processor.fetch_job_manifest", MagicMock())
@patch("backend.lambdas.jobs.stream_processor.json_lines_iterator")
def test_it_clears_queue(mock_json, mock_writer):
    mock_json.side_effect = [
        [{"DeletionQueueItemId": "id-1"}, {"DeletionQueueItemId": "id-2"}],
        [
            {"DeletionQueueItemId": "id-3"},
            {"DeletionQueueItemId": "id-4"},
            {"DeletionQueueItemId": "id-5"},
        ],
    ]
    mock_writer.return_value.__enter__.return_value = mock_writer
    clear_deletion_queue(
        {
            "Id": "job123",
            "Sk": "job123",
            "Type": "Job",
            "JobStatus": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
            "DeletionQueueSize": 1,
            "Manifests": [
                "s3://temp-bucket/manifests/job123/dm_01/manifest.json",
                "s3://temp-bucket/manifests/job123/dm_02/manifest.json",
            ],
        }
    )
    mock_writer.delete_item.assert_has_calls(
        [
            call(Key={"DeletionQueueItemId": "id-1"}),
            call(Key={"DeletionQueueItemId": "id-2"}),
            call(Key={"DeletionQueueItemId": "id-3"}),
            call(Key={"DeletionQueueItemId": "id-4"}),
            call(Key={"DeletionQueueItemId": "id-5"}),
        ],
        any_order=True,
    )
