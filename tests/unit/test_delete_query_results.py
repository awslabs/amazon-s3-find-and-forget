import os
from types import SimpleNamespace

import pytest
from mock import patch

with patch.dict(os.environ, {"BucketName": "test"}):
    from lambdas.src.tasks.delete_query_results import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("lambdas.src.tasks.delete_query_results.bucket")
def test_it_returns_all_deleted_results(bucket_mock):
    expected_result = [{'Key': 'test'}]
    bucket_mock.objects.filter.return_value = bucket_mock
    bucket_mock.delete.return_value = [{"Deleted": [{'Key': 'test'}]}]

    resp = handler({"ExecutionId": "test"}, SimpleNamespace())
    assert expected_result == resp


@patch("lambdas.src.tasks.delete_query_results.bucket")
def test_it_deletes_only_the_execution_id(bucket_mock):
    bucket_mock.objects.filter.return_value = bucket_mock
    bucket_mock.delete.return_value = [{"Deleted": [{'Key': 'test'}]}]

    handler({"ExecutionId": "test"}, SimpleNamespace())
    bucket_mock.objects.filter.assert_called_with(Prefix="test/")


@patch("lambdas.src.tasks.delete_query_results.bucket")
def test_it_throws_for_failed_deletions(bucket_mock):
    bucket_mock.objects.filter.return_value = bucket_mock
    bucket_mock.delete.return_value = [
        {"Deleted": [{'Key': 'test'}]},
        {"Errors": [{'Key': 'fail'}]},
    ]

    with pytest.raises(RuntimeError):
        handler({"ExecutionId": "test"}, SimpleNamespace())
