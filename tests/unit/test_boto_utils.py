import json
import os
import types
import pytest
from mock import MagicMock, ANY, patch

from boto_utils import paginate, batch_sqs_msgs, read_queue, create_stream_if_not_exists, log_event

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
		"MessageBody": json.dumps(x),
		"MessageGroupId": ANY
	} for x in range(0, 10)])
	queue.send_messages.assert_any_call(Entries=[{
		"Id": ANY,
		"MessageBody": json.dumps(x),
		"MessageGroupId": ANY
	} for x in range(10, 15)])


def test_it_passes_through_queue_args():
	queue = MagicMock()
	msgs = [1]
	batch_sqs_msgs(queue, msgs, DelaySeconds=60)
	queue.send_messages.assert_any_call(Entries=[{
		"DelaySeconds": 60,
		"Id": ANY,
		"MessageBody": ANY,
		"MessageGroupId": ANY
	}])


def test_it_sets_message_group_id_same_as_id():
	queue = MagicMock()
	msgs = [1]
	batch_sqs_msgs(queue, msgs)
	for call in queue.send_messages.call_args_list:
		args, kwargs = call
		for msg in kwargs['Entries']:
			assert msg["Id"] == msg["MessageGroupId"]


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


def test_it_creates_non_existent_streams():
	client = MagicMock()
	client.describe_log_streams.return_value = {"logStreams": []}
	resp = create_stream_if_not_exists(client, "log_group", "log_stream")
	assert None is resp
	client.create_log_stream.assert_called()


def test_it_returns_sequence_token_for_existing_streams():
	client = MagicMock()
	client.describe_log_streams.return_value = {"logStreams": [{"uploadSequenceToken": "test"}]}
	resp = create_stream_if_not_exists(client, "log_group", "log_stream")
	assert "test" is resp
	client.create_log_stream.assert_not_called()


def test_it_handles_existing_but_unwritten_streams():
	client = MagicMock()
	client.describe_log_streams.return_value = {"logStreams": [{}]}
	resp = create_stream_if_not_exists(client, "log_group", "log_stream")
	assert None is resp
	client.create_log_stream.assert_not_called()


@patch("boto_utils.create_stream_if_not_exists")
def test_it_logs_audit_events_for_new_streams(create_mock):
	client = MagicMock()
	create_mock.return_value = None
	log_event(client, "log_stream", "event_name", "data")
	client.put_log_events.assert_called_with(
		logGroupName=ANY,
		logStreamName="log_stream",
		logEvents=ANY,
	)
	assert {
		"EventName": "event_name",
		"EventData": "data"
	} == json.loads(client.put_log_events.call_args[1]["logEvents"][0]["message"])


@patch("boto_utils.create_stream_if_not_exists")
def test_it_uses_sequence_token_when_supplied(create_mock):
	client = MagicMock()
	create_mock.return_value = "some_token"
	log_event(client, "log_stream", "event_name", {"some": "data"})
	client.put_log_events.assert_called_with(
		logGroupName=ANY,
		logStreamName=ANY,
		logEvents=ANY,
		sequenceToken="some_token"
	)


@patch("boto_utils.create_stream_if_not_exists")
def test_it_defaults_log_group_name(create_mock):
	client = MagicMock()
	create_mock.return_value = None
	log_event(client, "log_stream", "event_name", {"some": "data"})
	client.put_log_events.assert_called_with(
		logGroupName="/aws/s3f2/",
		logStreamName=ANY,
		logEvents=ANY,
	)


@patch("boto_utils.create_stream_if_not_exists")
def test_it_allows_log_group_override(create_mock):
	client = MagicMock()
	create_mock.return_value = None
	with patch.dict(os.environ, {"LogGroupName": "/my/log/group/"}):
		log_event(client, "log_stream", "event_name", {"some": "data"})
	client.put_log_events.assert_called_with(
		logGroupName="/my/log/group/",
		logStreamName=ANY,
		logEvents=ANY,
	)
