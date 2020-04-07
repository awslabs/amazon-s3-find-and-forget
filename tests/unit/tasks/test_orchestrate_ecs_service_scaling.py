from datetime import datetime
from types import SimpleNamespace

import pytest
from mock import patch

from backend.lambdas.tasks.orchestrate_ecs_service_scaling import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.orchestrate_ecs_service_scaling.ecs")
def test_it_sets_count_to_zero_if_queue_is_empty(mock_client):
    event = {
        "Cluster": "cluster_name",
        "DeleteService": "delete_service",
        "DeletionTasksMaxNumber": 10,
        "QueueSize": 0
    }

    resp = handler(event, SimpleNamespace())
    mock_client.update_service.assert_called_with(
        cluster="cluster_name",
        desiredCount=0,
        service="delete_service")
    assert 0 == resp


@patch("backend.lambdas.tasks.orchestrate_ecs_service_scaling.ecs")
def test_it_sets_count_to_queue_size_if_less_than_max_tasks(mock_client):

    event = {
        "Cluster": "cluster_name",
        "DeleteService": "delete_service",
        "DeletionTasksMaxNumber": 10,
        "QueueSize": 5
    }

    resp = handler(event, SimpleNamespace())
    mock_client.update_service.assert_called_with(
        cluster="cluster_name",
        desiredCount=5,
        service="delete_service")
    assert 5 == resp


@patch("backend.lambdas.tasks.orchestrate_ecs_service_scaling.ecs")
def test_it_sets_count_to_max_tasks_if_more_than_max_tasks(mock_client):

    event = {
        "Cluster": "cluster_name",
        "DeleteService": "delete_service",
        "DeletionTasksMaxNumber": 10,
        "QueueSize": 35
    }

    resp = handler(event, SimpleNamespace())
    mock_client.update_service.assert_called_with(
        cluster="cluster_name",
        desiredCount=10,
        service="delete_service")
    assert 10 == resp
