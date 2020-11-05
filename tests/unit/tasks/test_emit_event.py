from types import SimpleNamespace

from mock import patch, ANY
import pytest
from backend.lambdas.tasks.emit_event import handler


pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.emit_event.emit_event")
def test_it_logs_event(mock_emit):
    """
    Emits logs to logs.

    Args:
        mock_emit: (todo): write your description
    """
    handler(
        {
            "JobId": "1234",
            "EmitterId": "5678",
            "EventName": "QueryFailed",
            "EventData": {"foo": "bar"},
        },
        SimpleNamespace(),
    )
    mock_emit.assert_called_with("1234", "QueryFailed", {"foo": "bar"}, "5678")


@patch("backend.lambdas.tasks.emit_event.emit_event")
@patch("backend.lambdas.tasks.emit_event.uuid4")
def test_it_defaults_emiiter_id(mock_uuid4, mock_emit):
    """
    Return a dict with mock id.

    Args:
        mock_uuid4: (todo): write your description
        mock_emit: (todo): write your description
    """
    mock_uuid4.return_value = "111"
    handler(
        {"JobId": "1234", "EventName": "QueryFailed", "EventData": {"foo": "bar"},},
        SimpleNamespace(),
    )
    mock_emit.assert_called_with("1234", "QueryFailed", {"foo": "bar"}, "111")
