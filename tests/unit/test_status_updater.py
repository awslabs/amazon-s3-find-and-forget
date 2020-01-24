import boto3
import pytest
from botocore.exceptions import ClientError
from mock import patch, Mock

from backend.lambdas.jobs.status_updater import update_status

pytestmark = [pytest.mark.unit, pytest.mark.jobs]


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_job_started(table):
    update_status("job123", [{
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "JobStarted",
        "EventData": {}
    }])
    table.update_item.assert_called_with(
        Key={
            'Id': "job123",
            'Sk': "job123",
        },
        UpdateExpression="set #JobStatus = :JobStatus, #JobStartTime = :JobStartTime",
        ConditionExpression="#JobStatus = :RUNNING OR #JobStatus = :QUEUED",
        ExpressionAttributeNames={
            '#JobStatus': 'JobStatus',
            '#JobStartTime': 'JobStartTime',
        },
        ExpressionAttributeValues={
            ':RUNNING': 'RUNNING',
            ':QUEUED': 'QUEUED',
            ':JobStatus': "RUNNING",
            ':JobStartTime': 123.0,
        },
        ReturnValues="UPDATED_NEW"
    )
    assert 1 == table.update_item.call_count


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_job_finished(table):
    update_status("job123", [{
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123,
        "EventName": "JobSucceeded",
        "EventData": {}
    }])
    table.update_item.assert_called_with(
        Key={
            'Id': "job123",
            'Sk': "job123",
        },
        UpdateExpression="set #JobStatus = :JobStatus, #JobFinishTime = :JobFinishTime",
        ConditionExpression="#JobStatus = :RUNNING OR #JobStatus = :QUEUED",
        ExpressionAttributeNames={
            '#JobStatus': 'JobStatus',
            '#JobFinishTime': 'JobFinishTime',
        },
        ExpressionAttributeValues={
            ':RUNNING': 'RUNNING',
            ':QUEUED': 'QUEUED',
            ':JobStatus': "COMPLETED",
            ':JobFinishTime': 123.0,
        },
        ReturnValues="UPDATED_NEW"
    )
    assert 1 == table.update_item.call_count


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_find_failed(table):
    update_status("job123", [{
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "FindPhaseFailed",
        "EventData": {}
    }])
    table.update_item.assert_called_with(
        Key={
            'Id': "job123",
            'Sk': "job123",
        },
        UpdateExpression="set #JobStatus = :JobStatus, #JobFinishTime = :JobFinishTime",
        ConditionExpression="#JobStatus = :RUNNING OR #JobStatus = :QUEUED",
        ExpressionAttributeNames={
            '#JobStatus': 'JobStatus',
            '#JobFinishTime': 'JobFinishTime',
        },
        ExpressionAttributeValues={
            ':JobStatus': "FIND_FAILED",
            ':RUNNING': 'RUNNING',
            ':QUEUED': 'QUEUED',
            ':JobFinishTime': 123.0,
        },
        ReturnValues="UPDATED_NEW"
    )
    assert 1 == table.update_item.call_count


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_forget_failed(table):
    update_status("job123", [{
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "ForgetPhaseFailed",
        "EventData": {}
    }])
    table.update_item.assert_called_with(
        Key={
            'Id': "job123",
            'Sk': "job123",
        },
        UpdateExpression="set #JobStatus = :JobStatus, #JobFinishTime = :JobFinishTime",
        ConditionExpression="#JobStatus = :RUNNING OR #JobStatus = :QUEUED",
        ExpressionAttributeNames={
            '#JobStatus': 'JobStatus',
            '#JobFinishTime': 'JobFinishTime',
        },
        ExpressionAttributeValues={
            ':JobStatus': "COMPLETED_WITH_ERRORS",
            ':RUNNING': 'RUNNING',
            ':QUEUED': 'QUEUED',
            ':JobFinishTime': 123.0,
        },
        ReturnValues="UPDATED_NEW"
    )
    assert 1 == table.update_item.call_count


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_exception(table):
    update_status("job123", [{
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "Exception",
        "EventData": {}
    }])
    table.update_item.assert_called_with(
        Key={
            'Id': "job123",
            'Sk': "job123",
        },
        UpdateExpression="set #JobStatus = :JobStatus, #JobFinishTime = :JobFinishTime",
        ConditionExpression="#JobStatus = :RUNNING OR #JobStatus = :QUEUED",
        ExpressionAttributeNames={
            '#JobStatus': 'JobStatus',
            '#JobFinishTime': 'JobFinishTime',
        },
        ExpressionAttributeValues={
            ':JobStatus': "FAILED",
            ':RUNNING': 'RUNNING',
            ':QUEUED': 'QUEUED',
            ':JobFinishTime': 123.0,
        },
        ReturnValues="UPDATED_NEW"
    )
    assert 1 == table.update_item.call_count


@patch("backend.lambdas.jobs.status_updater.ddb")
@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_already_failed_jobs(table, ddb):
    e = boto3.client("dynamodb").exceptions.ConditionalCheckFailedException
    ddb.meta.client.exceptions.ConditionalCheckFailedException = e
    table.update_item.side_effect = e({}, "ConditionalCheckFailedException")
    update_status("job123", [{
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "Exception",
        "EventData": {}
    }])
    table.update_item.assert_called()


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_throws_for_non_condition_errors(table):
    table.update_item.side_effect = ClientError({"Error": {"Code": "AnError"}}, "update_item")
    with pytest.raises(ClientError):
        update_status("job123", [{
            "Id": "job123",
            "Sk": "123456",
            "Type": "JobEvent",
            "CreatedAt": 123.0,
            "EventName": "Exception",
            "EventData": {}
        }])


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_ignores_none_status_events(table):
    update_status("job123", [{
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "SomeEvent",
        "EventData": {}
    }])
    table.update_item.assert_not_called()
