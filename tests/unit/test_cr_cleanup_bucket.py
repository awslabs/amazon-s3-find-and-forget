from types import SimpleNamespace

import json
import pytest
from mock import call, patch, MagicMock

from backend.lambdas.custom_resources.cleanup_bucket import create, delete, handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.cleanup_bucket.s3_client")
def test_it_does_nothing_on_create(mock_client):
    resp = create({}, MagicMock())

    mock_client.assert_not_called()
    assert not resp


@patch("backend.lambdas.custom_resources.cleanup_bucket.s3_client")
def test_it_removes_all_files_from_bucket(mock_client):
    event = {
        'ResourceProperties': {
            'WebUIBucket': 'webuibucket'
        }
    }

    mock_client.list_objects_v2.return_value = {
        'Contents': [
            {'Key': '/file.xyz'},
            {'Key': '/file2.xyz'},
            {'Key': '/path/to/file.xyz'},
        ]
    }

    resp = delete(event, MagicMock())

    mock_client.list_objects_v2.assert_called_with(
        Bucket="webuibucket"
    )

    mock_client.delete_object.assert_has_calls([
        call(Bucket="webuibucket", Key="/file.xyz"),
        call(Bucket="webuibucket", Key="/file2.xyz"),
        call(Bucket="webuibucket", Key="/path/to/file.xyz")])

    assert not resp

@patch("backend.lambdas.custom_resources.cleanup_bucket.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)
