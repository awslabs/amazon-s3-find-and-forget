import pytest
from mock import patch, MagicMock

from backend.lambdas.custom_resources.rerun_pipeline import delete, update, handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.custom_resources.rerun_pipeline.pipe_client")
def test_it_triggers_execution_if_web_ui_setting_changes(mock_client):
    event = {
        "ResourceProperties": {
            "DeployWebUI": "true",
            "PipelineName": "pipeline",
            "Version": "v0.60",
        },
        "OldResourceProperties": {"DeployWebUI": "false", "Version": "v0.60"},
    }

    resp = update(event, MagicMock())

    mock_client.start_pipeline_execution.assert_called_with(name="pipeline")

    assert not resp


@patch("backend.lambdas.custom_resources.rerun_pipeline.pipe_client")
def test_it_triggers_execution_if_new_version(mock_client):
    event = {
        "ResourceProperties": {
            "DeployWebUI": "true",
            "PipelineName": "pipeline",
            "Version": "v0.60",
        },
        "OldResourceProperties": {"DeployWebUI": "true", "Version": "v0.61"},
    }

    resp = update(event, MagicMock())

    mock_client.start_pipeline_execution.assert_called_with(name="pipeline")

    assert not resp


@patch("backend.lambdas.custom_resources.rerun_pipeline.pipe_client")
def test_it_triggers_execution_if_new_version_old_signature(mock_client):
    event = {
        "ResourceProperties": {
            "DeployWebUI": "true",
            "PipelineName": "pipeline",
            "Version": "v0.60",
        },
        "OldResourceProperties": {
            "DeployWebUI": "true"
        },  # Prior to 0.59, the Version parameter wasn't sent to the CR payload
    }

    resp = update(event, MagicMock())

    mock_client.start_pipeline_execution.assert_called_with(name="pipeline")

    assert not resp


@patch("backend.lambdas.custom_resources.rerun_pipeline.pipe_client")
def test_it_does_not_trigger_execution_if_not_required(mock_client):
    event = {
        "ResourceProperties": {"DeployWebUI": "false", "Version": "v0.60"},
        "OldResourceProperties": {"DeployWebUI": "false", "Version": "v0.60"},
    }

    resp = update(event, MagicMock())

    mock_client.assert_not_called()

    assert not resp


@patch("backend.lambdas.custom_resources.rerun_pipeline.pipe_client")
def test_it_does_nothing_on_delete(mock_client):
    resp = delete({}, MagicMock())

    mock_client.assert_not_called()
    assert not resp


@patch("backend.lambdas.custom_resources.rerun_pipeline.helper")
def test_it_delegates_to_cr_helper(cr_helper):
    handler(1, 2)
    cr_helper.assert_called_with(1, 2)
