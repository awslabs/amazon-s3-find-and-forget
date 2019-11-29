from types import SimpleNamespace

from mock import patch, ANY
import pytest
from backend.lambdas.tasks.log_audit_event import handler


pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.log_audit_event.log_event")
def test_it_logs_event(mock_log_event):
    handler({
        "JobId": "1234",
        "StreamSuffix": "5678",
        "EventName": "Query",
        "EventData": {"foo": "bar"}
    }, SimpleNamespace())
    mock_log_event.assert_called_with(ANY, "1234-5678", "Query", {"foo": "bar"})


@patch("backend.lambdas.tasks.log_audit_event.log_event")
@patch("backend.lambdas.tasks.log_audit_event.uuid4")
def test_it_defaults_stream_suffix(mock_uuid4, mock_log_event):
    mock_uuid4.return_value = 111
    handler({
        "JobId": "1234",
        "EventName": "Query",
        "EventData": {"foo": "bar"},
    }, SimpleNamespace())
    mock_log_event.assert_called_with(ANY, "1234-111", "Query", {"foo": "bar"})
