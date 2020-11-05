from types import SimpleNamespace

import json
import pytest
from mock import patch, MagicMock

from backend.lambdas.custom_resources.enable_container_insights import (
    create,
    delete,
    handler,
)

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.enable_container_insights.ecs_client")
@patch("os.getenv")
def test_it_updates_cluster_setting(getenv_mock, mock_client):
    """
    Return the custom cluster cluster updates.

    Args:
        getenv_mock: (todo): write your description
        mock_client: (todo): write your description
    """
    getenv_mock.return_value = "cluster-name"
    resp = create({}, MagicMock())
    mock_client.update_cluster_settings.assert_called_with(
        cluster="cluster-name",
        settings=[{"name": "containerInsights", "value": "enabled"}],
    )

    assert not resp


@patch("backend.lambdas.custom_resources.enable_container_insights.ecs_client")
def test_it_does_nothing_on_delete(mock_client):
    """
    Test if the mock is a mock.

    Args:
        mock_client: (todo): write your description
    """
    mock_client.return_value = MagicMock()
    resp = delete({}, MagicMock())
    mock_client.assert_not_called()
    assert resp == None


@patch("backend.lambdas.custom_resources.enable_container_insights.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    """
    Convert a cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr cr

    Args:
        cr_helper: (todo): write your description
    """
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)
