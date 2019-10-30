from datetime import datetime
from types import SimpleNamespace

import pytest
from mock import patch

from lambdas.src.tasks.check_query_status import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("lambdas.src.tasks.check_query_status.client")
def test_it_returns_query_status(mock_client):
    mock_client.get_query_execution.return_value = {
        'QueryExecution': {

            'Status': {
                'State': 'RUNNING',
                'SubmissionDateTime': datetime(2015, 1, 1),
                'CompletionDateTime': datetime(2015, 1, 1)
            },
        }
    }

    resp = handler("1234-5678-9012-3456", SimpleNamespace())
    assert {
        "State": "RUNNING",
        "Reason": "n/a",
    } == resp


@patch("lambdas.src.tasks.check_query_status.client")
def test_it_provides_reason_where_supplied(mock_client):
    mock_client.get_query_execution.return_value = {
        'QueryExecution': {
            'Status': {
                'State': 'FAILED',
                'StateChangeReason': 'Some reason',
                'SubmissionDateTime': datetime(2015, 1, 1),
                'CompletionDateTime': datetime(2015, 1, 1)
            },
        }
    }

    resp = handler("1234-5678-9012-3456", SimpleNamespace())
    assert {
        "State": "FAILED",
        "Reason": "Some reason",
    } == resp

