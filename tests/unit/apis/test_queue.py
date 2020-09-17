import json
import os
from types import SimpleNamespace
from decimal import Decimal

import pytest
from mock import patch, ANY

with patch.dict(os.environ, {"DeletionQueueTable": "DeletionQueueTable"}):
    from backend.lambdas.queue import handlers

pytestmark = [pytest.mark.unit, pytest.mark.api, pytest.mark.queue]

autorization_mock = {
    "authorizer": {
        "claims": {"sub": "cognitoSub", "cognito:username": "cognitoUsername"}
    }
}


@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_retrieves_all_items(table):
    table.scan.return_value = {"Items": []}
    response = handlers.get_handler({}, SimpleNamespace())
    assert {
        "statusCode": 200,
        "body": json.dumps({"MatchIds": [], "NextStart": None}),
        "headers": ANY,
    } == response
    table.scan.assert_called_with(Limit=10)


@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_retrieves_all_items_with_size_and_pagination(table):
    table.scan.return_value = {
        "Items": [
            {
                "DeletionQueueItemId": "id123",
                "MatchId": "foo",
                "DataMappers": [],
                "CreatedAt": 123456789,
            }
        ]
    }
    response = handlers.get_handler(
        {"queryStringParameters": {"page_size": "1", "start_at": "id000"}},
        SimpleNamespace(),
    )
    assert {
        "statusCode": 200,
        "body": json.dumps(
            {
                "MatchIds": [
                    {
                        "DeletionQueueItemId": "id123",
                        "MatchId": "foo",
                        "DataMappers": [],
                        "CreatedAt": 123456789,
                    }
                ],
                "NextStart": "id123",
            }
        ),
        "headers": ANY,
    } == response
    table.scan.assert_called_with(
        Limit=1, ExclusiveStartKey={"DeletionQueueItemId": "id000"}
    )


@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_adds_to_queue(table):
    response = handlers.enqueue_handler(
        {
            "body": json.dumps({"MatchId": "test", "DataMappers": ["a"]}),
            "requestContext": autorization_mock,
        },
        SimpleNamespace(),
    )

    assert 201 == response["statusCode"]
    assert {
        "DeletionQueueItemId": ANY,
        "MatchId": "test",
        "CreatedAt": ANY,
        "DataMappers": ["a"],
        "CreatedBy": {"Username": "cognitoUsername", "Sub": "cognitoSub"},
    } == json.loads(response["body"])


@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_adds_batch_to_queue(table):
    response = handlers.enqueue_batch_handler(
        {
            "body": json.dumps(
                {
                    "Matches": [
                        {"MatchId": "test", "DataMappers": ["a"]},
                        {"MatchId": "test2", "DataMappers": ["a"]},
                    ]
                }
            ),
            "requestContext": autorization_mock,
        },
        SimpleNamespace(),
    )

    assert 201 == response["statusCode"]
    assert {
        "Matches": [
            {
                "DeletionQueueItemId": ANY,
                "MatchId": "test",
                "CreatedAt": ANY,
                "DataMappers": ["a"],
                "CreatedBy": {"Username": "cognitoUsername", "Sub": "cognitoSub"},
            },
            {
                "DeletionQueueItemId": ANY,
                "MatchId": "test2",
                "CreatedAt": ANY,
                "DataMappers": ["a"],
                "CreatedBy": {"Username": "cognitoUsername", "Sub": "cognitoSub"},
            },
        ]
    } == json.loads(response["body"])


@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_provides_default_data_mappers(table):
    response = handlers.enqueue_handler(
        {"body": json.dumps({"MatchId": "test",}), "requestContext": autorization_mock},
        SimpleNamespace(),
    )

    assert 201 == response["statusCode"]
    assert {
        "DeletionQueueItemId": ANY,
        "MatchId": "test",
        "CreatedAt": ANY,
        "DataMappers": [],
        "CreatedBy": {"Username": "cognitoUsername", "Sub": "cognitoSub"},
    } == json.loads(response["body"])


@patch("backend.lambdas.queue.handlers.running_job_exists")
@patch("backend.lambdas.queue.handlers.deletion_queue_table")
def test_it_cancels_deletions(table, mock_running_job):
    mock_running_job.return_value = False
    response = handlers.cancel_handler(
        {"body": json.dumps({"Matches": [{"DeletionQueueItemId": "id123"}],})},
        SimpleNamespace(),
    )
    assert {"statusCode": 204, "headers": ANY} == response


@patch("backend.lambdas.queue.handlers.running_job_exists")
def test_it_prevents_cancelling_whilst_running_jobs(mock_running_job):
    mock_running_job.return_value = True
    response = handlers.cancel_handler(
        {
            "body": json.dumps(
                {"Matches": [{"MatchId": "test", "CreatedAt": 123456789,}],}
            )
        },
        SimpleNamespace(),
    )

    assert 400 == response["statusCode"]
    assert "headers" in response


@patch("backend.lambdas.queue.handlers.bucket_count", 1)
@patch("backend.lambdas.queue.handlers.paginate")
@patch("backend.lambdas.queue.handlers.uuid")
@patch("backend.lambdas.queue.handlers.jobs_table")
@patch("backend.lambdas.queue.handlers.running_job_exists")
@patch("backend.lambdas.queue.handlers.get_config")
def test_it_process_queue(mock_config, mock_running_job, job_table, uuid, paginate):
    mock_running_job.return_value = False
    mock_config.return_value = {
        "AthenaConcurrencyLimit": 15,
        "DeletionTasksMaxNumber": 50,
        "QueryExecutionWaitSeconds": 5,
        "QueryQueueWaitSeconds": 5,
        "ForgetQueueWaitSeconds": 30,
    }
    paginate.return_value = iter([{"MatchId": {"S": "123"}, "CreatedAt": {"N": "123"}}])
    uuid.uuid4.return_value = 123
    response = handlers.process_handler(
        {"body": "", "requestContext": autorization_mock}, SimpleNamespace()
    )
    job_table.put_item.assert_called_with(
        Item={
            "Id": "123",
            "Sk": "123",
            "Type": "Job",
            "JobStatus": "QUEUED",
            "GSIBucket": "0",
            "CreatedAt": ANY,
            "DeletionQueueItems": [{"MatchId": "123", "CreatedAt": 123}],
            "DeletionQueueItemsSkipped": False,
            "AthenaConcurrencyLimit": 15,
            "DeletionTasksMaxNumber": 50,
            "QueryExecutionWaitSeconds": 5,
            "QueryQueueWaitSeconds": 5,
            "ForgetQueueWaitSeconds": 30,
            "CreatedBy": {"Username": "cognitoUsername", "Sub": "cognitoSub"},
        }
    )
    assert 202 == response["statusCode"]
    assert "headers" in response
    assert {
        "Id": "123",
        "Sk": "123",
        "Type": "Job",
        "JobStatus": "QUEUED",
        "GSIBucket": "0",
        "CreatedAt": ANY,
        "DeletionQueueItems": [{"MatchId": "123", "CreatedAt": 123}],
        "DeletionQueueItemsSkipped": False,
        "AthenaConcurrencyLimit": 15,
        "DeletionTasksMaxNumber": 50,
        "QueryExecutionWaitSeconds": 5,
        "QueryQueueWaitSeconds": 5,
        "ForgetQueueWaitSeconds": 30,
        "CreatedBy": {"Username": "cognitoUsername", "Sub": "cognitoSub"},
    } == json.loads(response["body"])


@patch("backend.lambdas.queue.handlers.max_size_bytes", 300)
@patch("backend.lambdas.queue.handlers.bucket_count", 1)
@patch("backend.lambdas.queue.handlers.paginate")
@patch("backend.lambdas.queue.handlers.uuid")
@patch("backend.lambdas.queue.handlers.jobs_table")
@patch("backend.lambdas.queue.handlers.running_job_exists")
@patch("backend.lambdas.queue.handlers.get_config")
def test_it_partitions_queue(mock_config, mock_running_job, job_table, uuid, paginate):
    mock_running_job.return_value = False
    mock_config.return_value = {}

    deserialised_match = {
        "DataMappers": {"L": []},
        "MatchId": {"S": "123"},
        "CreatedAt": {"N": "1587992978"},
        "CreatedBy": {
            "M": {
                "Username": {"S": "foo@website.com"},
                "Sub": {"S": "123456789-123456789"},
            }
        },
    }

    serialised_match = {
        "MatchId": "123",
        "CreatedAt": 1587992978,
        "DataMappers": [],
        "CreatedBy": {"Username": "foo@website.com", "Sub": "123456789-123456789"},
    }

    paginate.return_value = iter([deserialised_match, deserialised_match])
    uuid.uuid4.return_value = 123
    response = handlers.process_handler(
        {"body": "", "requestContext": autorization_mock}, SimpleNamespace()
    )
    job_table.put_item.assert_called_with(
        Item={
            "Id": "123",
            "Sk": "123",
            "Type": "Job",
            "JobStatus": "QUEUED",
            "GSIBucket": "0",
            "CreatedAt": ANY,
            "DeletionQueueItems": [serialised_match],
            "DeletionQueueItemsSkipped": True,
            "CreatedBy": {"Username": "cognitoUsername", "Sub": "cognitoSub"},
        }
    )
    assert 202 == response["statusCode"]
    assert "headers" in response
    assert {
        "Id": "123",
        "Sk": "123",
        "Type": "Job",
        "JobStatus": "QUEUED",
        "GSIBucket": "0",
        "CreatedAt": ANY,
        "DeletionQueueItems": [serialised_match],
        "DeletionQueueItemsSkipped": True,
        "CreatedBy": {"Username": "cognitoUsername", "Sub": "cognitoSub"},
    } == json.loads(response["body"])


@patch("backend.lambdas.queue.handlers.bucket_count", 1)
@patch("backend.lambdas.queue.handlers.paginate")
@patch("backend.lambdas.queue.handlers.uuid")
@patch("backend.lambdas.queue.handlers.jobs_table")
@patch("backend.lambdas.queue.handlers.running_job_exists")
@patch("backend.lambdas.queue.handlers.get_config")
@patch("backend.lambdas.queue.handlers.utc_timestamp")
def test_it_applies_expiry(
    mock_utc, mock_config, mock_running_job, job_table, uuid, paginate
):
    mock_running_job.return_value = False
    mock_utc.return_value = 12346789
    mock_config.return_value = {
        "AthenaConcurrencyLimit": 15,
        "DeletionTasksMaxNumber": 50,
        "JobDetailsRetentionDays": 30,
        "QueryExecutionWaitSeconds": 5,
        "QueryQueueWaitSeconds": 5,
        "ForgetQueueWaitSeconds": 30,
    }
    paginate.return_value = iter([{"MatchId": {"S": "123"}, "CreatedAt": {"N": "123"}}])
    uuid.uuid4.return_value = 123
    response = handlers.process_handler(
        {"body": "", "requestContext": autorization_mock}, SimpleNamespace()
    )
    mock_utc.assert_called_with(days=30)
    job_table.put_item.assert_called_with(
        Item={
            "Id": "123",
            "Sk": "123",
            "Type": "Job",
            "JobStatus": "QUEUED",
            "GSIBucket": "0",
            "CreatedAt": ANY,
            "Expires": 12346789,
            "DeletionQueueItems": [{"MatchId": "123", "CreatedAt": 123}],
            "DeletionQueueItemsSkipped": False,
            "AthenaConcurrencyLimit": 15,
            "DeletionTasksMaxNumber": 50,
            "QueryExecutionWaitSeconds": 5,
            "QueryQueueWaitSeconds": 5,
            "ForgetQueueWaitSeconds": 30,
            "CreatedBy": {"Username": "cognitoUsername", "Sub": "cognitoSub"},
        }
    )
    assert 202 == response["statusCode"]


@patch("backend.lambdas.queue.handlers.running_job_exists")
def test_it_prevents_concurrent_running_jobs(mock_running_job):
    mock_running_job.return_value = True
    response = handlers.process_handler(
        {"body": "", "requestContext": autorization_mock}, SimpleNamespace()
    )

    assert 400 == response["statusCode"]
    assert "headers" in response


def test_it_calculates_ddb_item_size():
    scenarios = [
        [None, 0],
        [{"string": "test"}, 10],
        [{"int": 1234567}, 24],
        [{"zero": 0}, 25],
        [{"int": 1234567.892}, 24],
        [{"decimal": Decimal(1588080439)}, 28],
        [{"bool": False}, 5],
        [{"null": None}, 5],
        [{"arr": []}, 6],
        [{"arr": ["foo", "bar"]}, 12],
        [{"obj": {"foo": "bar"}}, 12],
        [{"obj": {}}, 6],
        [
            {
                "CreatedBy": {
                    "Username": "foo@website.com",
                    "Sub": "48265f68-ff51-471f-9702-c4ef18cf3d94",
                },
                "DataMappers": [],
                "MatchId": "jon_doe",
                "CreatedAt": Decimal(1587992978),
            },
            132,
        ],
    ]

    for scenario, result in scenarios:
        assert handlers.calculate_ddb_item_bytes(scenario) == result
