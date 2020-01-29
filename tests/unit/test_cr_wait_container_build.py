from types import SimpleNamespace
import json
import pytest
from mock import call, patch, MagicMock, Mock
from datetime import datetime, timezone

from backend.lambdas.custom_resources.wait_container_build import create, poll, handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.wait_container_build.s3_client")
@patch("backend.lambdas.custom_resources.wait_container_build.ecr_client")
def test_it_signal_readiness_when_image_ready(mock_ecr_client, mock_s3_client):
    event = {
        'ResourceProperties': {
            'ArtefactName': 'build/s3f2.zip',
            'CodeBuildArtefactBucket': 'codebuild-bucket',
            'ECRRepository': 'ecr-repo'
        }
    }

    mock_ecr_client.describe_images.return_value = {
        'imageDetails': [{
            'imagePushedAt': datetime(2020, 1, 6, 16, 12, 57, tzinfo=timezone.utc)
        }] 
    }
    mock_object = MagicMock()
    mock_s3_client.Object.return_value = mock_object
    mock_object.last_modified = datetime(2020, 1, 6, 16, 8, 51, tzinfo=timezone.utc)

    resp = poll(event, MagicMock())    

    assert resp


@patch("backend.lambdas.custom_resources.wait_container_build.s3_client")
@patch("backend.lambdas.custom_resources.wait_container_build.ecr_client")
def test_it_keeps_polling_when_image_not_ready(mock_ecr_client, mock_s3_client):
    event = {
        'ResourceProperties': {
            'ArtefactName': 'build/s3f2.zip',
            'CodeBuildArtefactBucket': 'codebuild-bucket',
            'ECRRepository': 'ecr-repo'
        }
    }

    mock_ecr_client.describe_images.return_value = {
        'imageDetails': [{
            'imagePushedAt': datetime(2020, 1, 6, 14, 0, 13, tzinfo=timezone.utc)
        }] 
    }
    mock_object = MagicMock()
    mock_s3_client.Object.return_value = mock_object
    mock_object.last_modified = datetime(2020, 1, 6, 16, 8, 51, tzinfo=timezone.utc)

    resp = poll(event, MagicMock())    

    assert not resp


@patch("backend.lambdas.custom_resources.wait_container_build.s3_client")
@patch("backend.lambdas.custom_resources.wait_container_build.ecr_client")
def test_it_keeps_polling_when_no_latest_image_found(mock_ecr_client, mock_s3_client):
    event = {
        'ResourceProperties': {
            'ArtefactName': 'build/s3f2.zip',
            'CodeBuildArtefactBucket': 'codebuild-bucket',
            'ECRRepository': 'ecr-repo'
        }
    }

    mock_ecr_client.describe_images.side_effect = Mock(side_effect=Exception('No image found for tag latest'))
    mock_object = MagicMock()
    mock_s3_client.Object.return_value = mock_object
    mock_object.last_modified = datetime(2020, 1, 6, 16, 8, 51, tzinfo=timezone.utc)

    resp = poll(event, MagicMock())    

    assert not resp


@patch("backend.lambdas.custom_resources.wait_container_build.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)

def test_it_does_nothing_on_create():
    assert create({}, MagicMock()) == None
