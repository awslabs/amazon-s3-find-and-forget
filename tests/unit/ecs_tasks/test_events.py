from mock import patch, MagicMock, mock_open

import json
import pytest

from backend.ecs_tasks.delete_files.events import sanitize_message, emit_deletion_event, \
    emit_failure_event, get_emitter_id

pytestmark = [pytest.mark.unit, pytest.mark.ecs_tasks]


@patch("backend.ecs_tasks.delete_files.events.emit_event")
@patch("backend.ecs_tasks.delete_files.events.get_emitter_id")
def test_it_emits_deletions(mock_get_id, mock_emit, message_stub):
    mock_get_id.return_value = "ECSTask_4567"
    stats_stub = {"Some": "stats"}
    msg = json.loads(message_stub())
    emit_deletion_event(msg, stats_stub)
    mock_emit.assert_called_with("1234", "ObjectUpdated", {
        "Statistics": stats_stub,
        "Object": "s3://bucket/path/basic.parquet",
    }, 'ECSTask_4567')


@patch("backend.ecs_tasks.delete_files.events.emit_event")
@patch("backend.ecs_tasks.delete_files.events.get_emitter_id")
def test_it_emits_failed_deletions(mock_get_id, mock_emit, message_stub):
    mock_get_id.return_value = "ECSTask_4567"
    msg = message_stub()
    emit_failure_event(msg, "Some error", "ObjectUpdateFailed")
    mock_emit.assert_called_with("1234", "ObjectUpdateFailed", {
        "Error": "Some error",
        "Message": json.loads(msg)
    }, 'ECSTask_4567')


@patch("backend.ecs_tasks.delete_files.events.emit_event")
@patch("backend.ecs_tasks.delete_files.events.get_emitter_id")
def test_it_emits_failed_rollback(mock_get_id, mock_emit, message_stub):
    mock_get_id.return_value = "ECSTask_4567"
    msg = message_stub()
    emit_failure_event(msg, "Some error", "ObjectRollbackFailed")
    mock_emit.assert_called_with("1234", "ObjectRollbackFailed", {
        "Error": "Some error",
        "Message": json.loads(msg)
    }, 'ECSTask_4567')


def test_it_raises_for_missing_job_id():
    with pytest.raises(ValueError):
        emit_failure_event("{}", "Some error", "deletion")


@patch("os.getenv", MagicMock(return_value="/some/path"))
@patch("os.path.isfile", MagicMock(return_value=True))
def test_it_loads_task_id_from_metadata():
    get_emitter_id.cache_clear()
    with patch("builtins.open", mock_open(read_data="{\"TaskARN\": \"arn:aws:ecs:us-west-2:012345678910:task/default/2b88376d-aba3-4950-9ddf-bcb0f388a40c\"}")):
        resp = get_emitter_id()
        assert "ECSTask_2b88376d-aba3-4950-9ddf-bcb0f388a40c" == resp


@patch("os.getenv", MagicMock(return_value=None))
def test_it_provides_default_id():
    get_emitter_id.cache_clear()
    resp = get_emitter_id()
    assert "ECSTask" == resp


def test_it_sanitises_matches(message_stub):
    assert "This message contains ID *** MATCH ID *** and *** MATCH ID ***" == sanitize_message(
        "This message contains ID 12345 and 23456", message_stub(Columns=[{
            "Column": "a", "MatchIds": ["12345", "23456", "34567"]
        }]))


def test_sanitiser_handles_malformed_messages():
    assert "an error message" == sanitize_message("an error message", "not json")
