import json
import types
import pytest
from mock import MagicMock, ANY

from boto_utils import paginate, batch_sqs_msgs

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
