import pytest
from mock import patch

from backend.lambdas.jobs.stats_updater import update_stats

pytestmark = [pytest.mark.unit, pytest.mark.jobs]


@patch("backend.lambdas.jobs.stats_updater.table")
def test_it_handles_successful_queries(table):
    update_stats({
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "QuerySucceeded",
        "EventData": {
            "QueryStatus": {
                "Statistics": {
                    "DataScannedInBytes": 10,
                    "EngineExecutionTimeInMillis": 100
                }
            }
        }
    })
    table.update_item.assert_called_with(
        Key={
            'Id': "job123",
            'Sk': "job123",
        },
        UpdateExpression="set #q = if_not_exists(#q, :z) + :q, "
                         "#qs = if_not_exists(#qs, :z) + :qs, "
                         "#f = if_not_exists(#f, :z) + :f, "
                         "#s = if_not_exists(#s, :z) + :s, "
                         "#t = if_not_exists(#t, :z) + :t",
        ExpressionAttributeNames={
            '#q': 'TotalQueryCount',
            '#qs': 'TotalQuerySucceededCount',
            '#f': 'TotalQueryFailedCount',
            '#s': 'TotalQueryScannedInBytes',
            '#t': 'TotalQueryTimeInMillis',
        },
        ExpressionAttributeValues={
            ':q': 1,
            ':qs': 1,
            ':f': 0,
            ':s': 10,
            ':t': 100,
            ':z': 0,
        },
        ReturnValues="UPDATED_NEW"
    )


@patch("backend.lambdas.jobs.stats_updater.table")
def test_it_handles_failed_queries(table):
    update_stats({
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
        UpdateExpression="set #q = if_not_exists(#q, :z) + :q, "
                         "#qs = if_not_exists(#qs, :z) + :qs, "
                         "#f = if_not_exists(#f, :z) + :f, "
                         "#s = if_not_exists(#s, :z) + :s, "
                         "#t = if_not_exists(#t, :z) + :t",
        ExpressionAttributeNames={
            '#q': 'TotalQueryCount',
            '#qs': 'TotalQuerySucceededCount',
            '#f': 'TotalQueryFailedCount',
            '#s': 'TotalQueryScannedInBytes',
            '#t': 'TotalQueryTimeInMillis',
        },
        ExpressionAttributeValues={
            ':q': 1,
            ':qs': 0,
            ':f': 1,
            ':s': 0,
            ':t': 0,
            ':z': 0,
        },
        ReturnValues="UPDATED_NEW"
    )


@patch("backend.lambdas.jobs.stats_updater.table")
def test_it_handles_successful_updates(table):
    update_stats({
        "Id": "job123",
        "Sk": "123456",
        "Type": "JobEvent",
        "CreatedAt": 123.0,
        "EventName": "ObjectUpdated",
        "EventData": {}
    })
    table.update_item.assert_called_with(
        Key={
            'Id': "job123",
            'Sk': 'job123'
        },
        UpdateExpression="set #c = if_not_exists(#c, :z) + :c, #f = if_not_exists(#f, :z) + :f",
        ExpressionAttributeNames={
            '#c': 'TotalObjectUpdatedCount',
            '#f': 'TotalObjectUpdateFailedCount',
        },
        ExpressionAttributeValues={
            ':c': 1,
            ':f': 0,
            ':z': 0,
        },
        ReturnValues="UPDATED_NEW"
    )


@patch("backend.lambdas.jobs.stats_updater.table")
def test_it_handles_failed_updates(table):
    update_stats({
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
            'Sk': 'job123'
        },
        UpdateExpression="set #c = if_not_exists(#c, :z) + :c, #f = if_not_exists(#f, :z) + :f",
        ExpressionAttributeNames={
            '#c': 'TotalObjectUpdatedCount',
            '#f': 'TotalObjectUpdateFailedCount',
        },
        ExpressionAttributeValues={
            ':c': 0,
            ':f': 1,
            ':z': 0,
        },
        ReturnValues="UPDATED_NEW"
    )
