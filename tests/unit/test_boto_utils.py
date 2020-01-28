import decimal
import json
import os
import types

import mock
import pytest
from mock import MagicMock, ANY, patch

from boto_utils import convert_iso8601_to_epoch, paginate, batch_sqs_msgs, read_queue, emit_event, DecimalEncoder, \
    normalise_dates

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
    queue.attributes = {}
    msgs = list(range(0, 15))
    batch_sqs_msgs(queue, msgs)
    queue.send_messages.assert_any_call(Entries=[{
        "Id": ANY,
        "MessageBody": json.dumps(x),
    } for x in range(0, 10)])
    queue.send_messages.assert_any_call(Entries=[{
        "Id": ANY,
        "MessageBody": json.dumps(x),
    } for x in range(10, 15)])


def test_it_passes_through_queue_args():
    queue = MagicMock()
    queue.attributes = {}
    msgs = [1]
    batch_sqs_msgs(queue, msgs, DelaySeconds=60)
    queue.send_messages.assert_any_call(Entries=[{
        "DelaySeconds": 60,
        "Id": ANY,
        "MessageBody": ANY,
    }])


def test_it_sets_message_group_id_where_queue_is_fifo():
    queue = MagicMock()
    queue.attributes = {"FifoQueue": True}
    msgs = [1]
    batch_sqs_msgs(queue, msgs)
    for call in queue.send_messages.call_args_list:
        args, kwargs = call
        for msg in kwargs['Entries']:
            assert "MessageGroupId" in msg


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
    queue.receive_messages.assert_any_call(MaxNumberOfMessages=10, AttributeNames=['All'])
    queue.receive_messages.assert_any_call(MaxNumberOfMessages=5, AttributeNames=['All'])


def test_it_handles_queue_with_less_msgs_than_desired():
    queue = MagicMock()
    queue.receive_messages.side_effect = [list(range(0, 2)), []]
    result = read_queue(queue, 10)
    assert [0, 1] == result


@patch("boto_utils.uuid.uuid4", MagicMock(return_value="1234"))
@patch("boto_utils.table")
def test_it_writes_events_to_ddb(mock_table):
    emit_event("job123", "event_name", "data", "emitter123", 123)
    mock_table.put_item.assert_called_with(
        Item={
            "Id": "job123",
            "Sk": "123000#1234",# gets converted to microseconds
            "Type": "JobEvent",
            "EventName": "event_name",
            "EventData": "data",
            "EmitterId": "emitter123",
            "CreatedAt": 123,
        }
    )


@patch("boto_utils.uuid.uuid4", MagicMock(return_value="1234"))
@patch("boto_utils.table")
def test_it_provides_defaults(mock_table):
    emit_event("job123", "event_name", "data")
    mock_table.put_item.assert_called_with(
        Item={
            "Id": "job123",
            "Sk": mock.ANY,
            "Type": "JobEvent",
            "EventName": "event_name",
            "EventData": "data",
            "EmitterId": "1234",
            "CreatedAt": mock.ANY,
        }
    )


def test_decimal_encoder():
    res_a = json.dumps({"k": decimal.Decimal(1.1)}, cls=DecimalEncoder)
    res_b = json.dumps({"k": decimal.Decimal(1.5)}, cls=DecimalEncoder)
    assert "{\"k\": 1}" == res_a
    assert "{\"k\": 2}" == res_b


def test_it_converts_sfn_datetimes_to_epoch():
    assert 1578327177 == convert_iso8601_to_epoch("2020-01-06T16:12:57.092Z")
    assert 1578327177 == convert_iso8601_to_epoch("2020-01-06T16:12:57Z")
    assert 1578327177 == convert_iso8601_to_epoch("2020-01-06T16:12:57+00:00")
    assert 1578327177 == convert_iso8601_to_epoch("2020-01-06T16:12:57.092+00:00")
    
    assert 1578323577 == convert_iso8601_to_epoch("2020-01-06T16:12:57.092+01:00")
    assert 1578323577 == convert_iso8601_to_epoch("2020-01-06T16:12:57+01:00")


def test_it_normalises_date_like_fields():
    assert {
       "a": [{"a": 1578327177, "b": "string"}],
       "b": [1578327177],
       "c": {"a": 1578327177},
       "d": 1578327177,
       "e": "string",
       "f": 2,
    } == normalise_dates({
       "a": [{"a": "2020-01-06T16:12:57.092Z", "b": "string"}],
       "b": ["2020-01-06T16:12:57.092Z"],
       "c": {"a": "2020-01-06T16:12:57.092Z"},
       "d": "2020-01-06T16:12:57.092Z",
       "e": "string",
       "f": 2,
    })
