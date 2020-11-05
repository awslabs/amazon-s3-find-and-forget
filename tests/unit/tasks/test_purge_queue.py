from types import SimpleNamespace

import pytest
from mock import patch, MagicMock

from backend.lambdas.tasks.purge_queue import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.purge_queue.sqs")
def test_it_purges_queue(mock_resource):
    """
    Test if the given mock queue.

    Args:
        mock_resource: (todo): write your description
    """
    mock_queue = MagicMock()
    mock_resource.Queue.return_value = mock_queue
    event = {"QueueUrl": "queue_url"}

    handler(event, SimpleNamespace())
    mock_queue.purge.assert_called()
