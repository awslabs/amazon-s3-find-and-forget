import pytest

from backend.lambdas.tasks.delete_message import handler
from mock import MagicMock, patch

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.delete_message.sqs")
def test_deletes_for_receipt_handle(sqs_mock):
    """
    Receives a mock.

    Args:
        sqs_mock: (todo): write your description
    """
    message_mock = MagicMock()
    sqs_mock.Message.return_value = message_mock
    handler({"ReceiptHandle": "test"}, MagicMock())
    message_mock.delete.assert_called()


@patch("backend.lambdas.tasks.delete_message.sqs")
def test_it_skips_if_no_receipt_handle(sqs_mock):
    """
    Test if a mock mock.

    Args:
        sqs_mock: (todo): write your description
    """
    message_mock = MagicMock()
    sqs_mock.Message.return_value = message_mock
    handler({}, MagicMock())
    message_mock.delete.assert_not_called()
