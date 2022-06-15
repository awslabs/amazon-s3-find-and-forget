import pytest
from mock import patch, MagicMock

from backend.lambdas.custom_resources.rerun_pipeline import create, update, handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.rerun_pipeline.pipe_client")
def test_it_triggers_execution_if_required(mock_client):
    event = {
        "ResourceProperties": {"DeployWebUI": "true", "PipelineName": "pipeline"},
        "OldResourceProperties": {"DeployWebUI": "false"},
    }

    resp = update(event, MagicMock())

    mock_client.start_pipeline_execution.assert_called_with(name="pipeline")

    assert not resp


@patch("backend.lambdas.custom_resources.rerun_pipeline.pipe_client")
def test_it_does_not_trigger_execution_if_not_required(mock_client):
    event = {
        "ResourceProperties": {"DeployWebUI": "false"},
        "OldResourceProperties": {"DeployWebUI": "false"},
    }

    resp = update(event, MagicMock())

    mock_client.assert_not_called()

    assert not resp


@patch("backend.lambdas.custom_resources.rerun_pipeline.pipe_client")
def test_it_does_nothing_on_create(mock_client):
    resp = create({}, MagicMock())

    mock_client.assert_not_called()
    assert not resp


@patch("backend.lambdas.custom_resources.rerun_pipeline.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)
