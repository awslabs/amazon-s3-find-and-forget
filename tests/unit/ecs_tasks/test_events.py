from urllib.error import URLError

from mock import patch, MagicMock

import json
import pytest

from backend.ecs_tasks.delete_files.events import (
    sanitize_message,
    emit_deletion_event,
    emit_failure_event,
    get_emitter_id,
)

pytestmark = [pytest.mark.unit, pytest.mark.ecs_tasks]


@patch("backend.ecs_tasks.delete_files.events.emit_event")
@patch("backend.ecs_tasks.delete_files.events.get_emitter_id")
def test_it_emits_deletions(mock_get_id, mock_emit, message_stub):
    mock_get_id.return_value = "ECSTask_4567"
    stats_stub = {"Some": "stats"}
    msg = json.loads(message_stub())
    emit_deletion_event(msg, stats_stub)
    mock_emit.assert_called_with(
        "1234",
        "ObjectUpdated",
        {"Statistics": stats_stub, "Object": "s3://bucket/path/basic.parquet",},
        "ECSTask_4567",
    )


@patch("backend.ecs_tasks.delete_files.events.emit_event")
@patch("backend.ecs_tasks.delete_files.events.get_emitter_id")
def test_it_emits_failed_deletions(mock_get_id, mock_emit, message_stub):
    mock_get_id.return_value = "ECSTask_4567"
    msg = message_stub()
    emit_failure_event(msg, "Some error", "ObjectUpdateFailed")
    mock_emit.assert_called_with(
        "1234",
        "ObjectUpdateFailed",
        {"Error": "Some error", "Message": json.loads(msg)},
        "ECSTask_4567",
    )


@patch("backend.ecs_tasks.delete_files.events.emit_event")
@patch("backend.ecs_tasks.delete_files.events.get_emitter_id")
def test_it_emits_failed_rollback(mock_get_id, mock_emit, message_stub):
    mock_get_id.return_value = "ECSTask_4567"
    msg = message_stub()
    emit_failure_event(msg, "Some error", "ObjectRollbackFailed")
    mock_emit.assert_called_with(
        "1234",
        "ObjectRollbackFailed",
        {"Error": "Some error", "Message": json.loads(msg)},
        "ECSTask_4567",
    )


def test_it_raises_for_missing_job_id():
    with pytest.raises(ValueError):
        emit_failure_event("{}", "Some error", "deletion")


@patch("os.getenv", MagicMock(return_value="http://metadatauri/path"))
@patch("urllib.request.urlopen")
def test_it_fetches_task_id_from_metadata_uri(url_open_mock):
    get_emitter_id.cache_clear()
    res = MagicMock()
    url_open_mock.return_value = res
    res.read.return_value = (
        b'{"Labels": {"com.amazonaws.ecs.task-arn": "arn/task-id"}}\n'
    )
    resp = get_emitter_id()
    assert "ECSTask_task-id" == resp
    url_open_mock.assert_called_with("http://metadatauri/path", timeout=1)


@patch("os.getenv", MagicMock(return_value="http://metadatauri/path"))
@patch("urllib.request.urlopen")
@patch("backend.ecs_tasks.delete_files.events.logger")
def test_it_defaults_task_id_if_urlerror(logger_mock, url_open_mock):
    get_emitter_id.cache_clear()
    res = MagicMock()
    url_open_mock.return_value = res
    res.read.side_effect = URLError("foo")
    resp = get_emitter_id()
    assert "ECSTask" == resp
    logger_mock.warning.assert_called_with(
        "Error when accessing the metadata service: foo"
    )


@patch("os.getenv", MagicMock(return_value="http://metadatauri/path"))
@patch("urllib.request.urlopen")
@patch("backend.ecs_tasks.delete_files.events.logger")
def test_it_defaults_task_id_if_malformed_response(logger_mock, url_open_mock):
    get_emitter_id.cache_clear()
    res = MagicMock()
    url_open_mock.return_value = res
    res.read.return_value = b"{}\n"
    resp = get_emitter_id()
    assert "ECSTask" == resp
    logger_mock.warning.assert_called_with(
        "Malformed response from the metadata service: b'{}\\n'"
    )


@patch("os.getenv", MagicMock(return_value="http://metadatauri/path"))
@patch("urllib.request.urlopen")
@patch("backend.ecs_tasks.delete_files.events.logger")
def test_it_defaults_task_id_if_generic_error(logger_mock, url_open_mock):
    get_emitter_id.cache_clear()
    res = MagicMock()
    url_open_mock.return_value = res
    res.read.side_effect = NameError("error")
    resp = get_emitter_id()
    assert "ECSTask" == resp
    logger_mock.warning.assert_called_with(
        "Error when getting emitter id from metadata service: error"
    )


@patch("os.getenv", MagicMock(return_value=None))
@patch("urllib.request.urlopen")
@patch("backend.ecs_tasks.delete_files.events.logger")
def test_it_defaults_task_id_if_env_variable_not_set(logger_mock, url_open_mock):
    get_emitter_id.cache_clear()
    resp = get_emitter_id()
    assert "ECSTask" == resp
    logger_mock.warning.assert_not_called()
    url_open_mock.assert_not_called()


def test_it_sanitises_matches(message_stub):
    assert (
        "This message contains ID *** MATCH ID *** and *** MATCH ID ***"
        == sanitize_message(
            "This message contains ID 12345 and 23456",
            message_stub(
                Columns=[{"Column": "a", "MatchIds": ["12345", "23456", "34567"]}]
            ),
        )
    )


def test_sanitiser_handles_malformed_messages():
    assert "an error message" == sanitize_message("an error message", "not json")
