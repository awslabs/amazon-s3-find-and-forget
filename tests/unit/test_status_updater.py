import boto3
import pytest
from botocore.exceptions import ClientError
from mock import patch

from backend.lambdas.jobs.status_updater import update_status

pytestmark = [pytest.mark.unit, pytest.mark.jobs]


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_job_started(table):
    update_status({
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "JobStarted",
        "EventData": {}
    })
    table.update_item.assert_called_with(
        Key={
            'Id': "job123",
            'Sk': "job123",
        },
        UpdateExpression="set #status = :s",
        ConditionExpression="#status = :r OR #status = :c OR #status = :q",
        ExpressionAttributeNames={
            '#status': 'JobStatus',
        },
        ExpressionAttributeValues={
            ':s': "RUNNING",
            ':r': "RUNNING",
            ':c': "COMPLETED",
            ':q': "QUEUED",
        },
        ReturnValues="UPDATED_NEW"
    )


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_job_finished(table):
    update_status({
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "JobSucceeded",
        "EventData": {}
    })
    table.update_item.assert_called_with(
        Key={
            'Id': "job123",
            'Sk': "job123",
        },
        UpdateExpression="set #status = :s",
        ConditionExpression="#status = :r OR #status = :c OR #status = :q",
        ExpressionAttributeNames={
            '#status': 'JobStatus',
        },
        ExpressionAttributeValues={
            ':s': "COMPLETED",
            ':r': "RUNNING",
            ':c': "COMPLETED",
            ':q': "QUEUED",
        },
        ReturnValues="UPDATED_NEW"
    )


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_query_failed(table):
    update_status({
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "QueryFailed",
        "EventData": {}
    })
    table.update_item.assert_called_with(
        Key={
            'Id': "job123",
            'Sk': "job123",
        },
        UpdateExpression="set #status = :s",
        ConditionExpression="#status = :r OR #status = :c OR #status = :q",
        ExpressionAttributeNames={
            '#status': 'JobStatus',
        },
        ExpressionAttributeValues={
            ':s': "ABORTED",
            ':r': "RUNNING",
            ':c': "COMPLETED",
            ':q': "QUEUED",
        },
        ReturnValues="UPDATED_NEW"
    )


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_update_failed(table):
    update_status({
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "ObjectUpdateFailed",
        "EventData": {}
    })
    table.update_item.assert_called_with(
        Key={
            'Id': "job123",
            'Sk': "job123",
        },
        UpdateExpression="set #status = :s",
        ConditionExpression="#status = :r OR #status = :c OR #status = :q",
        ExpressionAttributeNames={
            '#status': 'JobStatus',
        },
        ExpressionAttributeValues={
            ':s': "COMPLETED_WITH_ERRORS",
            ':r': "RUNNING",
            ':c': "COMPLETED",
            ':q': "QUEUED",
        },
        ReturnValues="UPDATED_NEW"
    )


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_exception(table):
    update_status({
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "Exception",
        "EventData": {}
    })
    table.update_item.assert_called_with(
        Key={
            'Id': "job123",
            'Sk': "job123",
        },
        UpdateExpression="set #status = :s",
        ConditionExpression="#status = :r OR #status = :c OR #status = :q",
        ExpressionAttributeNames={
            '#status': 'JobStatus',
        },
        ExpressionAttributeValues={
            ':s': "FAILED",
            ':r': "RUNNING",
            ':c': "COMPLETED",
            ':q': "QUEUED",
        },
        ReturnValues="UPDATED_NEW"
    )


@patch("backend.lambdas.jobs.status_updater.ddb")
@patch("backend.lambdas.jobs.status_updater.table")
def test_it_handles_already_failed_jobs(table, ddb):
    e = boto3.client("dynamodb").exceptions.ConditionalCheckFailedException
    ddb.meta.client.exceptions.ConditionalCheckFailedException = e
    table.update_item.side_effect = e({}, "ConditionalCheckFailedException")
    update_status({
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "JobSucceeded",
        "EventData": {}
    })
    table.update_item.assert_called()


@patch("backend.lambdas.jobs.status_updater.table")
def test_it_throws_for_non_condition_errors(table):
    table.update_item.side_effect = ClientError({"Error": {"Code": "AnError"}}, "update_item")
    with pytest.raises(ClientError):
        update_status({
            "Id": "job123",
            "Sk": "123456",
            "Type": "JobEvent",
            "CreatedAt": 123.0,
            "EventName": "JobSucceeded",
            "EventData": {}
        })
