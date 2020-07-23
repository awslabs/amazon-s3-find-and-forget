from types import SimpleNamespace

import pytest
from mock import patch

from backend.lambdas.tasks.check_task_count import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.check_task_count.client")
def test_it_returns_task_count(mock_client):
    mock_client.describe_services.return_value = {
        "services": [{"desiredCount": 0, "runningCount": 15, "pendingCount": 10,}]
    }

    event = {"Cluster": "test_cluster", "ServiceName": "test_service"}

    resp = handler(event, SimpleNamespace())
    assert {"Pending": 10, "Running": 15, "Total": 25} == resp


@patch("backend.lambdas.tasks.check_task_count.client")
def test_it_throws_for_invalid_service(mock_client):
    mock_client.describe_services.return_value = {"services": []}

    event = {"Cluster": "test_cluster", "ServiceName": "test_service"}

    with pytest.raises(ValueError):
        handler(event, SimpleNamespace())
