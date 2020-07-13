from types import SimpleNamespace

import pytest
from mock import patch, MagicMock

from backend.lambdas.tasks.check_queue_size import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.check_queue_size.sqs")
def test_it_returns_correct_queue_size(mock_resource):
    mock_queue = MagicMock()
    mock_resource.Queue.return_value = mock_queue
    mock_queue.attributes = {
        "ApproximateNumberOfMessages": "4",
        "ApproximateNumberOfMessagesNotVisible": "2",
    }

    event = {"QueueUrl": "queue_url"}

    resp = handler(event, SimpleNamespace())
    assert {"Visible": 4, "NotVisible": 2, "Total": 6} == resp
