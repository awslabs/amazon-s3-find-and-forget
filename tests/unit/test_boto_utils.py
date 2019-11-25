import json
import types
import pytest
from mock import MagicMock, ANY

from boto_utils import paginate, batch_sqs_msgs, read_queue

pytestmark = [pytest.mark.unit, pytest.mark.layers]


def test_it_paginates():
    client = MagicMock()
    client.get_paginator.return_value = client
    client.some_method.__name__ = "some_method"
    client.paginate.return_value = iter([{
        "Test": [
            "val"
        ]
    }])
    result = paginate(client, client.some_method, ["Test"])
    assert isinstance(result, types.GeneratorType)
    assert ["val"] == list(result)


def test_it_supports_single_iter_key():
    client = MagicMock()
    client.get_paginator.return_value = client
    client.some_method.__name__ = "some_method"
    client.paginate.return_value = iter([{
        "Test": [
            "val"
        ]
    }])
    result = paginate(client, client.some_method, "Test")
    assert isinstance(result, types.GeneratorType)
    assert ["val"] == list(result)


def test_it_batches_msgs():
    queue = MagicMock()
    msgs = list(range(0, 15))
    batch_sqs_msgs(queue, msgs)
    queue.send_messages.assert_any_call(Entries=[{
        "Id": ANY,
        "MessageBody": json.dumps(x)
    } for x in range(0, 10)])
    queue.send_messages.assert_any_call(Entries=[{
        "Id": ANY,
        "MessageBody": json.dumps(x)
    } for x in range(10, 15)])


def test_it_passes_through_queue_args():
    queue = MagicMock()
    msgs = [1]
    batch_sqs_msgs(queue, msgs, MessageGroupId="test")
    queue.send_messages.assert_any_call(Entries=[{
        "MessageGroupId": "test",
        "Id": ANY,
        "MessageBody": ANY
    }])


def test_it_truncates_received_messages_once_the_desired_amount_returned():
    queue = MagicMock()
    mock_list = [MagicMock() for i in range(0, 10)]
    queue.receive_messages.return_value = mock_list
    result = read_queue(queue, 2)
    assert 2 == len(result)
    assert 1 == queue.receive_messages.call_count


def test_it_handles_desired_number_of_msgs_greater_than_max_batch():
    queue = MagicMock()
    queue.receive_messages.side_effect = [list(range(0, 10)), list(range(0, 5))]
    read_queue(queue, 15)
    assert 2 == queue.receive_messages.call_count
    print(queue.receive_messages.call_args_list)
    queue.receive_messages.assert_any_call(MaxNumberOfMessages=10, AttributeNames=['All'])
    queue.receive_messages.assert_any_call(MaxNumberOfMessages=5, AttributeNames=['All'])


def test_it_handles_queue_with_less_msgs_than_desired():
    queue = MagicMock()
    queue.receive_messages.side_effect = [list(range(0, 2)), []]
    result = read_queue(queue, 10)
    assert [0, 1] == result
