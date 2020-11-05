import pytest
from mock import patch, MagicMock

from backend.lambdas.custom_resources.copy_build_artefact import create, delete, handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.copy_build_artefact.s3_client")
def test_it_copies_file(mock_client):
    """
    Test if the mock. github_file.

    Args:
        mock_client: (todo): write your description
    """
    event = {
        "ResourceProperties": {
            "ArtefactName": "build/s3f2.zip",
            "CodeBuildArtefactBucket": "codebuild-bucket",
            "PreBuiltArtefactsBucket": "source-bucket-eu-west-1",
            "Version": "1.0",
        }
    }

    resp = create(event, MagicMock())

    mock_client.copy_object.assert_called_with(
        Bucket="codebuild-bucket",
        CopySource="source-bucket-eu-west-1/amazon-s3-find-and-forget/1.0/build.zip",
        Key="build/s3f2.zip",
    )

    assert resp == "arn:aws:s3:::codebuild-bucket/build/s3f2.zip"


@patch("backend.lambdas.custom_resources.copy_build_artefact.s3_client")
def test_it_does_nothing_on_delete(mock_client):
    """
    Test if the mock.

    Args:
        mock_client: (todo): write your description
    """
    event = {
        "ResourceProperties": {
            "ArtefactName": "build/s3f2.zip",
            "CodeBuildArtefactBucket": "source-bucket-eu-west-1",
            "PreBuiltArtefactsBucket": "codebuild-bucket",
            "Version": "1.0",
        }
    }
    resp = delete(event, MagicMock())
    mock_client.assert_not_called()
    assert not resp


@patch("backend.lambdas.custom_resources.copy_build_artefact.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    """
    Convert a cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr

    Args:
        cr_helper: (todo): write your description
    """
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)
