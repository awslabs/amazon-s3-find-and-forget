import os
from types import SimpleNamespace

import pytest
from mock import patch, ANY

with patch.dict(os.environ, {"QueueUrl": "test"}):
    from backend.lambdas.tasks.submit_query_results import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.submit_query_results.batch_sqs_msgs")
@patch("backend.lambdas.tasks.submit_query_results.paginate")
def test_it_returns_only_paths(paginate_mock, batch_sqs_msgs_mock):
    paginate_mock.return_value = iter([
        {
            "Data": [
                {
                    "VarCharValue": "$path"
                },
            ]
        },
        {
            "Data": [
                {
                    "VarCharValue": "s3://mybucket/mykey1"
                },
            ]
        },
        {
            "Data": [
                {
                    "VarCharValue": "s3://mybucket/mykey2"
                },
            ]
        },
    ])
    columns = [{
      "Column": "customer_id",
      "MatchIds": [
        "2732559"
      ]
    }]

    resp = handler({"JobId": "1234", "QueryId": "123", "Columns": columns}, SimpleNamespace())
    assert [
        "s3://mybucket/mykey1",
        "s3://mybucket/mykey2",
    ] == resp


@patch("backend.lambdas.tasks.submit_query_results.batch_sqs_msgs")
@patch("backend.lambdas.tasks.submit_query_results.paginate")
def test_it_submits_results_to_be_batched(paginate_mock, batch_sqs_msgs_mock):
    paginate_mock.return_value = iter([
        {
            "Data": [
                {
                    "VarCharValue": "$path"
                },
            ]
        },
        {
            "Data": [
                {
                    "VarCharValue": "s3://mybucket/mykey1"
                },
            ]
        },
        {
            "Data": [
                {
                    "VarCharValue": "s3://mybucket/mykey2"
                },
            ]
        },
    ])
    columns = [{
      "Column": "customer_id",
      "MatchIds": [
        "2732559"
      ]
    }]

    handler({"JobId": "1234", "QueryId": "123", "Columns": columns}, SimpleNamespace())
    batch_sqs_msgs_mock.assert_called_with(ANY, [
        {"JobId": "1234", "Columns": columns, "Object": "s3://mybucket/mykey1"},
        {"JobId": "1234", "Columns": columns, "Object": "s3://mybucket/mykey2"},
    ])


@patch("backend.lambdas.tasks.submit_query_results.batch_sqs_msgs")
@patch("backend.lambdas.tasks.submit_query_results.paginate")
def test_it_propagates_role_arn(paginate_mock, batch_sqs_msgs_mock):
    paginate_mock.return_value = iter([
        {
            "Data": [
                {
                    "VarCharValue": "$path"
                },
            ]
        },
        {
            "Data": [
                {
                    "VarCharValue": "s3://mybucket/mykey1"
                },
            ]
        },
    ])
    columns = [{
      "Column": "customer_id",
      "MatchIds": [
        "2732559"
      ]
    }]

    handler({
        "RoleArn": "arn:aws:iam:accountid:role/rolename",
        "JobId": "1234",
        "QueryId": "123",
        "Columns": columns
    }, SimpleNamespace())
    batch_sqs_msgs_mock.assert_called_with(ANY, [
        {
            "JobId": "1234", "Columns": columns,
            "Object": "s3://mybucket/mykey1",
            "RoleArn": "arn:aws:iam:accountid:role/rolename"},
    ])
