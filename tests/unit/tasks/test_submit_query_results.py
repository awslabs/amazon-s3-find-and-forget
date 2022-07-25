import os
from types import SimpleNamespace

import pytest
from mock import call, patch, ANY

with patch.dict(os.environ, {"QueueUrl": "test"}):
    from backend.lambdas.tasks.submit_query_results import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]

test_data = [
    {"Data": [{"VarCharValue": "$path"}]},
    {"Data": [{"VarCharValue": "s3://mybucket/mykey1"}]},
    {"Data": [{"VarCharValue": "s3://mybucket/mykey2"}]},
]


@patch("backend.lambdas.tasks.submit_query_results.batch_sqs_msgs")
@patch("backend.lambdas.tasks.submit_query_results.paginate")
def test_it_returns_only_paths(paginate_mock, batch_sqs_msgs_mock):
    paginate_mock.return_value = iter(test_data)
    columns = [{"Column": "customer_id", "MatchIds": ["2732559"]}]

    resp = handler(
        {
            "JobId": "1234",
            "QueryId": "123",
            "Columns": columns,
            "DeleteOldVersions": False,
        },
        SimpleNamespace(),
    )
    assert 2 == resp


@patch("backend.lambdas.tasks.submit_query_results.batch_sqs_msgs")
@patch("backend.lambdas.tasks.submit_query_results.paginate")
def test_it_submits_results_to_be_batched(paginate_mock, batch_sqs_msgs_mock):
    paginate_mock.return_value = iter(test_data)
    columns = [{"Column": "customer_id", "MatchIds": ["2732559"]}]

    handler(
        {
            "JobId": "1234",
            "QueryId": "123",
            "Columns": columns,
        },
        SimpleNamespace(),
    )
    batch_sqs_msgs_mock.assert_called_with(
        ANY,
        [
            {
                "JobId": "1234",
                "Columns": columns,
                "Object": "s3://mybucket/mykey1",
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
            },
            {
                "JobId": "1234",
                "Columns": columns,
                "Object": "s3://mybucket/mykey2",
                "DeleteOldVersions": True,
                "IgnoreObjectNotFoundExceptions": False,
            },
        ],
    )


@patch("backend.lambdas.tasks.submit_query_results.batch_sqs_msgs")
@patch("backend.lambdas.tasks.submit_query_results.paginate")
@patch("backend.lambdas.tasks.submit_query_results.MSG_BATCH_SIZE", 2)
def test_it_submits_results_to_be_batched_multiple_times_according_to_batch_size(
    paginate_mock, batch_sqs_msgs_mock
):
    paginate_mock.return_value = iter(
        [*test_data, {"Data": [{"VarCharValue": "s3://mybucket/mykey3"}]}]
    )
    columns = [{"Column": "customer_id", "MatchIds": ["2732559"]}]

    handler(
        {
            "JobId": "1234",
            "QueryId": "123",
            "Columns": columns,
        },
        SimpleNamespace(),
    )

    assert batch_sqs_msgs_mock.call_args_list == [
        call(
            ANY,
            [
                {
                    "JobId": "1234",
                    "Columns": columns,
                    "Object": "s3://mybucket/mykey1",
                    "DeleteOldVersions": True,
                    "IgnoreObjectNotFoundExceptions": False,
                },
                {
                    "JobId": "1234",
                    "Columns": columns,
                    "Object": "s3://mybucket/mykey2",
                    "DeleteOldVersions": True,
                    "IgnoreObjectNotFoundExceptions": False,
                },
            ],
        ),
        call(
            ANY,
            [
                {
                    "JobId": "1234",
                    "Columns": columns,
                    "Object": "s3://mybucket/mykey3",
                    "DeleteOldVersions": True,
                    "IgnoreObjectNotFoundExceptions": False,
                }
            ],
        ),
    ]


@patch("backend.lambdas.tasks.submit_query_results.batch_sqs_msgs")
@patch("backend.lambdas.tasks.submit_query_results.paginate")
def test_it_propagates_optional_properties(paginate_mock, batch_sqs_msgs_mock):
    paginate_mock.return_value = iter(test_data)
    columns = [{"Column": "customer_id", "MatchIds": ["2732559"]}]

    handler(
        {
            "RoleArn": "arn:aws:iam:accountid:role/rolename",
            "DeleteOldVersions": False,
            "JobId": "1234",
            "QueryId": "123",
            "Columns": columns,
            "IgnoreObjectNotFoundExceptions": True,
        },
        SimpleNamespace(),
    )
    batch_sqs_msgs_mock.assert_called_with(
        ANY,
        [
            {
                "JobId": "1234",
                "Columns": columns,
                "Object": "s3://mybucket/mykey1",
                "RoleArn": "arn:aws:iam:accountid:role/rolename",
                "DeleteOldVersions": False,
                "IgnoreObjectNotFoundExceptions": True,
            },
            {
                "JobId": "1234",
                "Columns": columns,
                "Object": "s3://mybucket/mykey2",
                "RoleArn": "arn:aws:iam:accountid:role/rolename",
                "DeleteOldVersions": False,
                "IgnoreObjectNotFoundExceptions": True,
            },
        ],
    )
