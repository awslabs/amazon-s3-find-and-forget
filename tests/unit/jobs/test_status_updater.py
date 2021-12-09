import os

import boto3
import pytest
from botocore.exceptions import ClientError
from mock import patch, Mock

with patch.dict(os.environ, {"JobTable": "test", "DeletionQueueTable": "test"}):
    from backend.lambdas.jobs.status_updater import (
        update_status,
        determine_status,
        job_has_errors,
    )

pytestmark = [pytest.mark.unit, pytest.mark.jobs]


@patch("backend.lambdas.jobs.status_updater.job_has_errors", Mock(return_value=False))
def test_it_determines_basic_statuses():
    assert "FIND_FAILED" == determine_status("123", "FindPhaseFailed")
    assert "FORGET_FAILED" == determine_status("123", "ForgetPhaseFailed")
    assert "FAILED" == determine_status("123", "Exception")
    assert "RUNNING" == determine_status("123", "JobStarted")
    assert "FORGET_COMPLETED_CLEANUP_IN_PROGRESS" == determine_status(
        "123", "ForgetPhaseEnded"
    )
    assert "COMPLETED_CLEANUP_FAILED" == determine_status("123", "CleanupFailed")
    assert "COMPLETED" == determine_status("123", "CleanupSucceeded")


@patch("backend.lambdas.jobs.status_updater.job_has_errors", Mock(return_value=True))
def test_it_determines_completed_with_errors():
    assert "FORGET_PARTIALLY_FAILED" == determine_status("123", "ForgetPhaseEnded")


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_determines_job_has_errors_for_failed_object_updates(table):
    table.get_item.return_value = {"Item": {"TotalObjectUpdateFailedCount": 1}}
    assert job_has_errors("test")


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_determines_job_has_errors_for_failed_queries(table):
    table.get_item.return_value = {"Item": {"TotalQueryFailedCount": 1}}
    assert job_has_errors("test")


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_determines_job_does_not_have_errors_for_failed_object_updates(table):
    table.get_item.return_value = {
        "Item": {"TotalObjectUpdateFailedCount": 0, "TotalQueryFailedCount": 0,}
    }
    assert not job_has_errors("test")


@patch(
    "backend.lambdas.jobs.status_updater.determine_status", Mock(return_value="RUNNING")
)
@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_job_started(table):
    update_status(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "JobStarted",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123",},
        UpdateExpression="set #JobStatus = :JobStatus, #JobStartTime = :JobStartTime",
        ConditionExpression="#Id = :Id AND #Sk = :Sk AND (#JobStatus = :RUNNING OR #JobStatus = :QUEUED OR #JobStatus = :FORGET_COMPLETED_CLEANUP_IN_PROGRESS)",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#JobStatus": "JobStatus",
            "#JobStartTime": "JobStartTime",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":RUNNING": "RUNNING",
            ":QUEUED": "QUEUED",
            ":FORGET_COMPLETED_CLEANUP_IN_PROGRESS": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
            ":JobStatus": "RUNNING",
            ":JobStartTime": 123.0,
        },
        ReturnValues="ALL_NEW",
    )
    assert 1 == table.update_item.call_count


@patch(
    "backend.lambdas.jobs.status_updater.determine_status",
    Mock(return_value="FORGET_COMPLETED_CLEANUP_IN_PROGRESS"),
)
@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_forget_finished(table):
    update_status(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123,
                "EventName": "ForgetPhaseEnded",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123",},
        UpdateExpression="set #JobStatus = :JobStatus",
        ConditionExpression="#Id = :Id AND #Sk = :Sk AND (#JobStatus = :RUNNING OR #JobStatus = :QUEUED OR #JobStatus = :FORGET_COMPLETED_CLEANUP_IN_PROGRESS)",
        ExpressionAttributeNames={"#Id": "Id", "#Sk": "Sk", "#JobStatus": "JobStatus",},
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":RUNNING": "RUNNING",
            ":QUEUED": "QUEUED",
            ":FORGET_COMPLETED_CLEANUP_IN_PROGRESS": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
            ":JobStatus": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
        },
        ReturnValues="ALL_NEW",
    )
    assert 1 == table.update_item.call_count


@patch(
    "backend.lambdas.jobs.status_updater.determine_status",
    Mock(return_value="COMPLETED"),
)
@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_cleanup_success(table):
    update_status(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123,
                "EventName": "CleanupSucceeded",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123",},
        UpdateExpression="set #JobStatus = :JobStatus, #JobFinishTime = :JobFinishTime",
        ConditionExpression="#Id = :Id AND #Sk = :Sk AND (#JobStatus = :RUNNING OR #JobStatus = :QUEUED OR #JobStatus = :FORGET_COMPLETED_CLEANUP_IN_PROGRESS)",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#JobStatus": "JobStatus",
            "#JobFinishTime": "JobFinishTime",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":RUNNING": "RUNNING",
            ":QUEUED": "QUEUED",
            ":FORGET_COMPLETED_CLEANUP_IN_PROGRESS": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
            ":JobStatus": "COMPLETED",
            ":JobFinishTime": 123.0,
        },
        ReturnValues="ALL_NEW",
    )
    assert 1 == table.update_item.call_count


@patch(
    "backend.lambdas.jobs.status_updater.determine_status",
    Mock(return_value="COMPLETED_CLEANUP_FAILED"),
)
@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_cleanup_failed(table):
    update_status(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123,
                "EventName": "CleanupFailed",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123",},
        UpdateExpression="set #JobStatus = :JobStatus, #JobFinishTime = :JobFinishTime",
        ConditionExpression="#Id = :Id AND #Sk = :Sk AND (#JobStatus = :RUNNING OR #JobStatus = :QUEUED OR #JobStatus = :FORGET_COMPLETED_CLEANUP_IN_PROGRESS)",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#JobStatus": "JobStatus",
            "#JobFinishTime": "JobFinishTime",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":RUNNING": "RUNNING",
            ":QUEUED": "QUEUED",
            ":FORGET_COMPLETED_CLEANUP_IN_PROGRESS": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
            ":JobStatus": "COMPLETED_CLEANUP_FAILED",
            ":JobFinishTime": 123.0,
        },
        ReturnValues="ALL_NEW",
    )
    assert 1 == table.update_item.call_count


@patch(
    "backend.lambdas.jobs.status_updater.determine_status",
    Mock(return_value="FIND_FAILED"),
)
@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_find_failed(table):
    update_status(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "FindPhaseFailed",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123",},
        UpdateExpression="set #JobStatus = :JobStatus, #JobFinishTime = :JobFinishTime",
        ConditionExpression="#Id = :Id AND #Sk = :Sk AND (#JobStatus = :RUNNING OR #JobStatus = :QUEUED OR #JobStatus = :FORGET_COMPLETED_CLEANUP_IN_PROGRESS)",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#JobStatus": "JobStatus",
            "#JobFinishTime": "JobFinishTime",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":JobStatus": "FIND_FAILED",
            ":RUNNING": "RUNNING",
            ":QUEUED": "QUEUED",
            ":FORGET_COMPLETED_CLEANUP_IN_PROGRESS": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
            ":JobFinishTime": 123.0,
        },
        ReturnValues="ALL_NEW",
    )
    assert 1 == table.update_item.call_count


@patch(
    "backend.lambdas.jobs.status_updater.determine_status",
    Mock(return_value="FORGET_FAILED"),
)
@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_forget_failed(table):
    update_status(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "ForgetPhaseFailed",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123",},
        UpdateExpression="set #JobStatus = :JobStatus, #JobFinishTime = :JobFinishTime",
        ConditionExpression="#Id = :Id AND #Sk = :Sk AND (#JobStatus = :RUNNING OR #JobStatus = :QUEUED OR #JobStatus = :FORGET_COMPLETED_CLEANUP_IN_PROGRESS)",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#JobStatus": "JobStatus",
            "#JobFinishTime": "JobFinishTime",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":JobStatus": "FORGET_FAILED",
            ":RUNNING": "RUNNING",
            ":QUEUED": "QUEUED",
            ":FORGET_COMPLETED_CLEANUP_IN_PROGRESS": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
            ":JobFinishTime": 123.0,
        },
        ReturnValues="ALL_NEW",
    )
    assert 1 == table.update_item.call_count


@patch(
    "backend.lambdas.jobs.status_updater.determine_status", Mock(return_value="FAILED")
)
@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_exception(table):
    update_status(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "Exception",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123",},
        UpdateExpression="set #JobStatus = :JobStatus, #JobFinishTime = :JobFinishTime",
        ConditionExpression="#Id = :Id AND #Sk = :Sk AND (#JobStatus = :RUNNING OR #JobStatus = :QUEUED OR #JobStatus = :FORGET_COMPLETED_CLEANUP_IN_PROGRESS)",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#JobStatus": "JobStatus",
            "#JobFinishTime": "JobFinishTime",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":JobStatus": "FAILED",
            ":RUNNING": "RUNNING",
            ":QUEUED": "QUEUED",
            ":FORGET_COMPLETED_CLEANUP_IN_PROGRESS": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
            ":JobFinishTime": 123.0,
        },
        ReturnValues="ALL_NEW",
    )
    assert 1 == table.update_item.call_count


@patch("backend.lambdas.jobs.status_updater.ddb")
@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_already_failed_jobs(table, ddb):
    e = boto3.client("dynamodb").exceptions.ConditionalCheckFailedException
    ddb.meta.client.exceptions.ConditionalCheckFailedException = e
    table.update_item.side_effect = e({}, "ConditionalCheckFailedException")
    update_status(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "Exception",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_called()


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_throws_for_non_condition_errors(table):
    table.update_item.side_effect = ClientError(
        {"Error": {"Code": "AnError"}}, "update_item"
    )
    with pytest.raises(ClientError):
        update_status(
            "job123",
            [
                {
                    "Id": "job123",
                    "Sk": "123456",
                    "Type": "JobEvent",
                    "CreatedAt": 123.0,
                    "EventName": "Exception",
                    "EventData": {},
                }
            ],
        )


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_ignores_none_status_events(table):
    update_status(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "SomeEvent",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_not_called()


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_query_planning_complete(table):
    update_status(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "QueryPlanningComplete",
                "EventData": {
                    "GeneratedQueries": 123,
                    "DeletionQueueSize": 3456,
                    "Manifests": [
                        "s3://temp-bucket/manifests/job123/dm-123/manifest.json"
                    ],
                },
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123",},
        UpdateExpression="set #GeneratedQueries = :GeneratedQueries, #DeletionQueueSize = :DeletionQueueSize, #Manifests = :Manifests",
        ConditionExpression="#Id = :Id AND #Sk = :Sk AND (#JobStatus = :RUNNING OR #JobStatus = :QUEUED OR #JobStatus = :FORGET_COMPLETED_CLEANUP_IN_PROGRESS)",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#JobStatus": "JobStatus",
            "#GeneratedQueries": "GeneratedQueries",
            "#DeletionQueueSize": "DeletionQueueSize",
            "#Manifests": "Manifests",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":GeneratedQueries": 123,
            ":DeletionQueueSize": 3456,
            ":Manifests": ["s3://temp-bucket/manifests/job123/dm-123/manifest.json"],
            ":RUNNING": "RUNNING",
            ":QUEUED": "QUEUED",
            ":FORGET_COMPLETED_CLEANUP_IN_PROGRESS": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
        },
        ReturnValues="ALL_NEW",
    )
    assert 1 == table.update_item.call_count
