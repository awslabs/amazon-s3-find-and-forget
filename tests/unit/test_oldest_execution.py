import os
from types import SimpleNamespace

import pytest
from mock import patch

with patch.dict(os.environ, {"StateMachineArn": "test"}):
    from lambdas.src.tasks.oldest_execution import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("lambdas.src.tasks.oldest_execution.paginate")
def test_it_returns_true_where_is_oldest_execution(paginate_mock):
    latest_exec = {"executionArn": "arn:aws:states:eu-west-1:123456789012:execution:HelloWorld:testA"}
    oldest_exec = {"executionArn": "arn:aws:states:eu-west-1:123456789012:execution:HelloWorld:testB"}
    paginate_mock.return_value = iter([latest_exec, oldest_exec])

    resp = handler({
        "ExecutionId": oldest_exec["executionArn"]
    }, SimpleNamespace())
    assert resp is True


@patch("lambdas.src.tasks.oldest_execution.paginate")
def test_it_returns_false_where_not_oldest(paginate_mock):
    latest_exec = {"executionArn": "arn:aws:states:eu-west-1:123456789012:execution:HelloWorld:testA"}
    oldest_exec = {"executionArn": "arn:aws:states:eu-west-1:123456789012:execution:HelloWorld:testB"}
    paginate_mock.return_value = iter([latest_exec, oldest_exec])

    resp = handler({
        "ExecutionId": latest_exec["executionArn"]
    }, SimpleNamespace())
    assert resp is False
