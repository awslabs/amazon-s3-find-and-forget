import pytest
from mock import patch, MagicMock

from backend.lambdas.custom_resources.cleanup_bucket import create, delete, handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.cleanup_bucket.s3")
def test_it_does_nothing_on_create(mock_client):
    resp = create({}, MagicMock())

    mock_client.assert_not_called()
    assert not resp


@patch("backend.lambdas.custom_resources.cleanup_bucket.s3")
def test_it_removes_all_objects_from_bucket_when_no_versioning(mock_client):
    event = {"ResourceProperties": {"Bucket": "webuibucket"}}

    all_method = MagicMock()
    all_method2 = MagicMock()
    bucket = MagicMock()
    bucket.objects = MagicMock()
    bucket.object_versions = MagicMock()
    bucket.objects.all.return_value = all_method
    bucket.object_versions.all.return_value = all_method2
    mock_client.Bucket.return_value = bucket

    resp = delete(event, MagicMock())
    all_method.delete.assert_called_with()
    all_method2.delete.assert_called_with()

    assert not resp


@patch("backend.lambdas.custom_resources.cleanup_bucket.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)
