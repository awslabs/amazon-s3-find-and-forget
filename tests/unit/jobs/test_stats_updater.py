import boto3
import pytest
from mock import patch

from backend.lambdas.jobs.stats_updater import update_stats

pytestmark = [pytest.mark.unit, pytest.mark.jobs]


@patch("backend.lambdas.jobs.stats_updater.table")
def test_it_handles_successful_queries(table):
    """
    Test if queries queries to be added queries.

    Args:
        table: (str): write your description
    """
    resp = update_stats(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "QuerySucceeded",
                "EventData": {
                    "Statistics": {
                        "DataScannedInBytes": 10,
                        "EngineExecutionTimeInMillis": 100,
                    }
                },
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123",},
        ConditionExpression="#Id = :Id AND #Sk = :Sk",
        UpdateExpression="set #qt = if_not_exists(#qt, :z) + :qt, "
        "#qs = if_not_exists(#qs, :z) + :qs, "
        "#qf = if_not_exists(#qf, :z) + :qf, "
        "#qb = if_not_exists(#qb, :z) + :qb, "
        "#qm = if_not_exists(#qm, :z) + :qm, "
        "#ou = if_not_exists(#ou, :z) + :ou, "
        "#of = if_not_exists(#of, :z) + :of, "
        "#or = if_not_exists(#or, :z) + :or",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#qt": "TotalQueryCount",
            "#qs": "TotalQuerySucceededCount",
            "#qf": "TotalQueryFailedCount",
            "#qb": "TotalQueryScannedInBytes",
            "#qm": "TotalQueryTimeInMillis",
            "#ou": "TotalObjectUpdatedCount",
            "#of": "TotalObjectUpdateFailedCount",
            "#or": "TotalObjectRollbackFailedCount",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":qt": 1,
            ":qs": 1,
            ":qf": 0,
            ":qb": 10,
            ":qm": 100,
            ":ou": 0,
            ":of": 0,
            ":or": 0,
            ":z": 0,
        },
        ReturnValues="ALL_NEW",
    )


@patch("backend.lambdas.jobs.stats_updater.table")
def test_it_handles_failed_queries(table):
    """
    Test for failed failed queries table.

    Args:
        table: (str): write your description
    """
    resp = update_stats(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "QueryFailed",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123",},
        ConditionExpression="#Id = :Id AND #Sk = :Sk",
        UpdateExpression="set #qt = if_not_exists(#qt, :z) + :qt, "
        "#qs = if_not_exists(#qs, :z) + :qs, "
        "#qf = if_not_exists(#qf, :z) + :qf, "
        "#qb = if_not_exists(#qb, :z) + :qb, "
        "#qm = if_not_exists(#qm, :z) + :qm, "
        "#ou = if_not_exists(#ou, :z) + :ou, "
        "#of = if_not_exists(#of, :z) + :of, "
        "#or = if_not_exists(#or, :z) + :or",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#qt": "TotalQueryCount",
            "#qs": "TotalQuerySucceededCount",
            "#qf": "TotalQueryFailedCount",
            "#qb": "TotalQueryScannedInBytes",
            "#qm": "TotalQueryTimeInMillis",
            "#ou": "TotalObjectUpdatedCount",
            "#of": "TotalObjectUpdateFailedCount",
            "#or": "TotalObjectRollbackFailedCount",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":qt": 1,
            ":qs": 0,
            ":qf": 1,
            ":qb": 0,
            ":qm": 0,
            ":ou": 0,
            ":of": 0,
            ":or": 0,
            ":z": 0,
        },
        ReturnValues="ALL_NEW",
    )


@patch("backend.lambdas.jobs.stats_updater.table")
def test_it_handles_successful_updates(table):
    """
    Test for updates the updates

    Args:
        table: (todo): write your description
    """
    resp = update_stats(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "ObjectUpdated",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123"},
        ConditionExpression="#Id = :Id AND #Sk = :Sk",
        UpdateExpression="set #qt = if_not_exists(#qt, :z) + :qt, "
        "#qs = if_not_exists(#qs, :z) + :qs, "
        "#qf = if_not_exists(#qf, :z) + :qf, "
        "#qb = if_not_exists(#qb, :z) + :qb, "
        "#qm = if_not_exists(#qm, :z) + :qm, "
        "#ou = if_not_exists(#ou, :z) + :ou, "
        "#of = if_not_exists(#of, :z) + :of, "
        "#or = if_not_exists(#or, :z) + :or",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#qt": "TotalQueryCount",
            "#qs": "TotalQuerySucceededCount",
            "#qf": "TotalQueryFailedCount",
            "#qb": "TotalQueryScannedInBytes",
            "#qm": "TotalQueryTimeInMillis",
            "#ou": "TotalObjectUpdatedCount",
            "#of": "TotalObjectUpdateFailedCount",
            "#or": "TotalObjectRollbackFailedCount",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":qt": 0,
            ":qs": 0,
            ":qf": 0,
            ":qb": 0,
            ":qm": 0,
            ":ou": 1,
            ":of": 0,
            ":or": 0,
            ":z": 0,
        },
        ReturnValues="ALL_NEW",
    )


@patch("backend.lambdas.jobs.stats_updater.table")
def test_it_handles_failed_updates(table):
    """
    Test for updates that updates in the updates table.

    Args:
        table: (str): write your description
    """
    resp = update_stats(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "ObjectUpdateFailed",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123"},
        ConditionExpression="#Id = :Id AND #Sk = :Sk",
        UpdateExpression="set #qt = if_not_exists(#qt, :z) + :qt, "
        "#qs = if_not_exists(#qs, :z) + :qs, "
        "#qf = if_not_exists(#qf, :z) + :qf, "
        "#qb = if_not_exists(#qb, :z) + :qb, "
        "#qm = if_not_exists(#qm, :z) + :qm, "
        "#ou = if_not_exists(#ou, :z) + :ou, "
        "#of = if_not_exists(#of, :z) + :of, "
        "#or = if_not_exists(#or, :z) + :or",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#qt": "TotalQueryCount",
            "#qs": "TotalQuerySucceededCount",
            "#qf": "TotalQueryFailedCount",
            "#qb": "TotalQueryScannedInBytes",
            "#qm": "TotalQueryTimeInMillis",
            "#ou": "TotalObjectUpdatedCount",
            "#of": "TotalObjectUpdateFailedCount",
            "#or": "TotalObjectRollbackFailedCount",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":qt": 0,
            ":qs": 0,
            ":qf": 0,
            ":qb": 0,
            ":qm": 0,
            ":ou": 0,
            ":of": 1,
            ":or": 0,
            ":z": 0,
        },
        ReturnValues="ALL_NEW",
    )


@patch("backend.lambdas.jobs.stats_updater.table")
def test_it_handles_failed_rollbacks(table):
    """
    Test for rollbacks

    Args:
        table: (str): write your description
    """
    resp = update_stats(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "ObjectRollbackFailed",
                "EventData": {},
            }
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123"},
        ConditionExpression="#Id = :Id AND #Sk = :Sk",
        UpdateExpression="set #qt = if_not_exists(#qt, :z) + :qt, "
        "#qs = if_not_exists(#qs, :z) + :qs, "
        "#qf = if_not_exists(#qf, :z) + :qf, "
        "#qb = if_not_exists(#qb, :z) + :qb, "
        "#qm = if_not_exists(#qm, :z) + :qm, "
        "#ou = if_not_exists(#ou, :z) + :ou, "
        "#of = if_not_exists(#of, :z) + :of, "
        "#or = if_not_exists(#or, :z) + :or",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#qt": "TotalQueryCount",
            "#qs": "TotalQuerySucceededCount",
            "#qf": "TotalQueryFailedCount",
            "#qb": "TotalQueryScannedInBytes",
            "#qm": "TotalQueryTimeInMillis",
            "#ou": "TotalObjectUpdatedCount",
            "#of": "TotalObjectUpdateFailedCount",
            "#or": "TotalObjectRollbackFailedCount",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":qt": 0,
            ":qs": 0,
            ":qf": 0,
            ":qb": 0,
            ":qm": 0,
            ":ou": 0,
            ":of": 0,
            ":or": 1,
            ":z": 0,
        },
        ReturnValues="ALL_NEW",
    )


@patch("backend.lambdas.jobs.stats_updater.table")
def test_it_handles_multiple_events(table):
    """
    Test for cross - section stats.

    Args:
        table: (str): write your description
    """
    resp = update_stats(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "QuerySucceeded",
                "EventData": {
                    "Statistics": {
                        "DataScannedInBytes": 10,
                        "EngineExecutionTimeInMillis": 100,
                    }
                },
            },
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "ObjectUpdated",
                "EventData": {},
            },
        ],
    )
    table.update_item.assert_called_with(
        Key={"Id": "job123", "Sk": "job123"},
        ConditionExpression="#Id = :Id AND #Sk = :Sk",
        UpdateExpression="set #qt = if_not_exists(#qt, :z) + :qt, "
        "#qs = if_not_exists(#qs, :z) + :qs, "
        "#qf = if_not_exists(#qf, :z) + :qf, "
        "#qb = if_not_exists(#qb, :z) + :qb, "
        "#qm = if_not_exists(#qm, :z) + :qm, "
        "#ou = if_not_exists(#ou, :z) + :ou, "
        "#of = if_not_exists(#of, :z) + :of, "
        "#or = if_not_exists(#or, :z) + :or",
        ExpressionAttributeNames={
            "#Id": "Id",
            "#Sk": "Sk",
            "#qt": "TotalQueryCount",
            "#qs": "TotalQuerySucceededCount",
            "#qf": "TotalQueryFailedCount",
            "#qb": "TotalQueryScannedInBytes",
            "#qm": "TotalQueryTimeInMillis",
            "#ou": "TotalObjectUpdatedCount",
            "#of": "TotalObjectUpdateFailedCount",
            "#or": "TotalObjectRollbackFailedCount",
        },
        ExpressionAttributeValues={
            ":Id": "job123",
            ":Sk": "job123",
            ":qt": 1,
            ":qs": 1,
            ":qf": 0,
            ":qb": 10,
            ":qm": 100,
            ":ou": 1,
            ":of": 0,
            ":or": 0,
            ":z": 0,
        },
        ReturnValues="ALL_NEW",
    )


@patch("backend.lambdas.jobs.stats_updater.ddb")
@patch("backend.lambdas.jobs.stats_updater.table")
def test_it_handles_already_failed_jobs(table, ddb):
    """
    Test for failed jobs failed.

    Args:
        table: (str): write your description
        ddb: (todo): write your description
    """
    e = boto3.client("dynamodb").exceptions.ConditionalCheckFailedException
    ddb.meta.client.exceptions.ConditionalCheckFailedException = e
    table.update_item.side_effect = e({}, "ConditionalCheckFailedException")
    update_stats(
        "job123",
        [
            {
                "Id": "job123",
                "Sk": "123456",
                "Type": "JobEvent",
                "CreatedAt": 123.0,
                "EventName": "QuerySucceeded",
                "EventData": {
                    "Statistics": {
                        "DataScannedInBytes": 10,
                        "EngineExecutionTimeInMillis": 100,
                    }
                },
            }
        ],
    )
    table.update_item.assert_called()
