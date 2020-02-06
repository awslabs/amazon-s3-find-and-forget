import pytest
from mock import patch, MagicMock

from backend.lambdas.custom_resources.cleanup_repository import create, delete, handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.cleanup_repository.ecr_client")
def test_it_does_nothing_on_create(mock_client):
    resp = create({}, MagicMock())

    mock_client.assert_not_called()
    assert not resp


@patch("backend.lambdas.custom_resources.cleanup_repository.ecr_client")
@patch("backend.lambdas.custom_resources.cleanup_repository.paginate")
def test_it_removes_all_images_from_ecr_repository(paginate_mock, mock_client):
    event = {
        'ResourceProperties': {
            'Repository': 'myrepository'
        }
    }

    paginate_mock.return_value = iter([
        {
            'imageDigest': '12345',
            'imageTag': 'tag12345'
        },
        {
            'imageDigest': '67890',
            'imageTag': 'tag67890'
        }
    ])

    resp = delete(event, MagicMock())

    mock_client.batch_delete_image.assert_called_with(imageIds=[
        {
            'imageDigest': '12345',
            'imageTag': 'tag12345'
        },
        {
            'imageDigest': '67890',
            'imageTag': 'tag67890'
        }
    ], repositoryName='myrepository')

    assert not resp


@patch("backend.lambdas.custom_resources.cleanup_repository.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)
