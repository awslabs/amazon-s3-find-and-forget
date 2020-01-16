from types import SimpleNamespace

import json
import pytest
from mock import call, patch, MagicMock

from backend.lambdas.custom_resources.copy_backend_artefact import create, delete, handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.copy_backend_artefact.s3_client")
def test_it_copies_file(mock_client):
    event = {
        'ResourceProperties': {
            'ArtefactName': 'build/s3f2.zip',
            'CodeBuildArtifactBucket': 'codebuild-bucket',
            'PreBuiltArtefactsBucket': 'source-bucket-eu-west-1',
            'Version': '1.0'
        }
    }

    resp = create(event, MagicMock())

    mock_client.copy_object.assert_called_with(
        Bucket="codebuild-bucket",
        CopySource="source-bucket-eu-west-1/amazon-s3-find-and-forget/1.0/backend.zip",
        Key="build/s3f2.zip")

    assert "arn:aws:s3:::codebuild-bucket/build/s3f2.zip" == resp


@patch("backend.lambdas.custom_resources.copy_backend_artefact.s3_client")
def test_it_does_nothing_on_delete(mock_client):
    event = {
        'ResourceProperties': {
            'ArtefactName': 'build/s3f2.zip',
            'CodeBuildArtifactBucket': 'source-bucket-eu-west-1',
            'PreBuiltArtefactsBucket': 'codebuild-bucket',
            'Version': '1.0'
        }
    }
    resp = delete(event, MagicMock())
    mock_client.assert_not_called()
    assert not resp


@patch("backend.lambdas.custom_resources.copy_backend_artefact.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)
