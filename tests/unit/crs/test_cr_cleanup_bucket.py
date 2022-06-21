import pytest
from mock import patch, MagicMock

from backend.lambdas.custom_resources.cleanup_bucket import (
    create,
    update,
    delete,
    empty_bucket,
    handler,
)

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.cleanup_bucket.s3")
def test_it_does_nothing_on_create(mock_client):
    resp = create({}, MagicMock())

    mock_client.assert_not_called()
    assert not resp


@patch("backend.lambdas.custom_resources.cleanup_bucket.s3")
def test_it_removes_all_objects_from_bucket_when_no_versioning(mock_client):
    bucket_name = "webuibucket"

    all_method = MagicMock()
    all_method2 = MagicMock()
    bucket = MagicMock()
    bucket.objects = MagicMock()
    bucket.object_versions = MagicMock()
    bucket.objects.all.return_value = all_method
    bucket.object_versions.all.return_value = all_method2
    mock_client.Bucket.return_value = bucket

    resp = empty_bucket(bucket_name)
    all_method.delete.assert_called_with()
    all_method2.delete.assert_called_with()

    assert not resp


@patch("backend.lambdas.custom_resources.cleanup_bucket.empty_bucket")
def test_it_removes_all_objects_from_bucket_on_delete(mock_empty_bucket):
    event = {"ResourceProperties": {"Bucket": "webuibucket"}}

    resp = delete(event, MagicMock())

    mock_empty_bucket.assert_called_with("webuibucket")

    assert not resp


@patch("backend.lambdas.custom_resources.cleanup_bucket.empty_bucket")
def test_it_removes_all_objects_from_bucket_on_update_if_required(mock_empty_bucket):
    event = {
        "ResourceProperties": {"DeployWebUI": "false", "Bucket": "webuibucket"},
        "OldResourceProperties": {"DeployWebUI": "true", "Bucket": "webuibucket"},
    }

    resp = update(event, MagicMock())

    mock_empty_bucket.assert_called_with("webuibucket")

    assert not resp


@patch("backend.lambdas.custom_resources.cleanup_bucket.empty_bucket")
def test_it_removes_all_objects_from_bucket_on_update_if_not_required(
    mock_empty_bucket,
):
    event = {
        "ResourceProperties": {"DeployWebUI": "false"},
        "OldResourceProperties": {"DeployWebUI": "false"},
    }

    resp = update(event, MagicMock())

    mock_empty_bucket.assert_not_called()

    assert not resp


@patch("backend.lambdas.custom_resources.cleanup_bucket.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)
