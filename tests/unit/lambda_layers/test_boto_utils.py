import datetime
import decimal
import json
import types
import mock

import pytest
from boto3.session import Session
from botocore.exceptions import ClientError
from mock import MagicMock, ANY, patch

from boto_utils import (
    convert_iso8601_to_epoch,
    paginate,
    batch_sqs_msgs,
    read_queue,
    emit_event,
    DecimalEncoder,
    normalise_dates,
    deserialize_item,
    running_job_exists,
    get_config,
    utc_timestamp,
    get_job_expiry,
    parse_s3_url,
    get_user_info,
    get_session,
    fetch_job_manifest,
    json_lines_iterator,
)

pytestmark = [pytest.mark.unit, pytest.mark.layers]


def test_it_paginates():
    client = MagicMock()
    client.get_paginator.return_value = client
    client.some_method.__name__ = "some_method"
    client.paginate.return_value = iter([{"Test": ["val"]}])
    result = paginate(client, client.some_method, ["Test"], an="arg")
    assert isinstance(result, types.GeneratorType)
    assert ["val"] == list(result)


def test_it_supports_single_iter_key():
    client = MagicMock()
    client.get_paginator.return_value = client
    client.some_method.__name__ = "some_method"
    client.paginate.return_value = iter([{"Test": ["val"]}])
    result = paginate(client, client.some_method, "Test")
    assert isinstance(result, types.GeneratorType)
    assert ["val"] == list(result)


def test_it_supports_multiple_iter_keys():
    client = MagicMock()
    client.get_paginator.return_value = client
    client.some_method.__name__ = "some_method"
    client.paginate.return_value = iter(
        [
            {
                "A": [
                    "valA1",
                    "valA2",
                ],
                "B": [
                    "valB1",
                    "valB2",
                ],
            }
        ]
    )
    result = paginate(client, client.some_method, ["A", "B"])
    assert isinstance(result, types.GeneratorType)
    assert [("valA1", "valB1"), ("valA2", "valB2")] == list(result)


def test_it_supports_multiple_iter_keys_of_varying_lengths():
    client = MagicMock()
    client.get_paginator.return_value = client
    client.some_method.__name__ = "some_method"
    client.paginate.return_value = iter(
        [
            {
                "A": [
                    "valA1",
                    "valA2",
                ],
                "B": [
                    "valB1",
                ],
            }
        ]
    )
    result = paginate(client, client.some_method, ["A", "B"])
    assert isinstance(result, types.GeneratorType)
    assert [("valA1", "valB1"), ("valA2", None)] == list(result)


def test_it_supports_nested_iter_keys():
    client = MagicMock()
    client.get_paginator.return_value = client
    client.some_method.__name__ = "some_method"
    client.paginate.return_value = iter(
        [
            {
                "A": {
                    "B": [
                        "valAB1",
                        "valAB2",
                    ],
                },
                "C": [
                    "valC1",
                ],
            }
        ]
    )
    result = paginate(client, client.some_method, ["A.B", "C"])
    assert isinstance(result, types.GeneratorType)
    assert [("valAB1", "valC1"), ("valAB2", None)] == list(result)


def test_it_batches_msgs():
    queue = MagicMock()
    queue.attributes = {}
    msgs = list(range(0, 15))
    batch_sqs_msgs(queue, msgs)
    queue.send_messages.assert_any_call(
        Entries=[
            {
                "Id": ANY,
                "MessageBody": json.dumps(x),
            }
            for x in range(0, 10)
        ]
    )
    queue.send_messages.assert_any_call(
        Entries=[
            {
                "Id": ANY,
                "MessageBody": json.dumps(x),
            }
            for x in range(10, 15)
        ]
    )


def test_it_passes_through_queue_args():
    queue = MagicMock()
    queue.attributes = {}
    msgs = [1]
    batch_sqs_msgs(queue, msgs, DelaySeconds=60)
    queue.send_messages.assert_any_call(
        Entries=[
            {
                "DelaySeconds": 60,
                "Id": ANY,
                "MessageBody": ANY,
            }
        ]
    )


def test_it_sets_message_group_id_where_queue_is_fifo():
    queue = MagicMock()
    queue.attributes = {"FifoQueue": True}
    msgs = [1]
    batch_sqs_msgs(queue, msgs)
    for call in queue.send_messages.call_args_list:
        args, kwargs = call
        for msg in kwargs["Entries"]:
            assert "MessageGroupId" in msg


def test_it_truncates_received_messages_once_the_desired_amount_returned():
    queue = MagicMock()
    mock_list = [MagicMock() for i in range(0, 10)]
    queue.receive_messages.return_value = mock_list
    result = read_queue(queue, 2)
    assert len(result) == 2
    assert queue.receive_messages.call_count == 1


def test_it_handles_desired_number_of_msgs_greater_than_max_batch():
    queue = MagicMock()
    queue.receive_messages.side_effect = [list(range(0, 10)), list(range(0, 5))]
    read_queue(queue, 15)
    assert queue.receive_messages.call_count == 2
    queue.receive_messages.assert_any_call(
        MaxNumberOfMessages=10, AttributeNames=["All"]
    )
    queue.receive_messages.assert_any_call(
        MaxNumberOfMessages=5, AttributeNames=["All"]
    )


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
            "Sk": "123000#1234",  # gets converted to microseconds
            "Type": "JobEvent",
            "EventName": "event_name",
            "EventData": "data",
            "EmitterId": "emitter123",
            "CreatedAt": 123,
            "Expires": mock.ANY,
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
            "Expires": mock.ANY,
        }
    )


def test_decimal_encoder():
    res_a = json.dumps({"k": decimal.Decimal(1.1)}, cls=DecimalEncoder)
    res_b = json.dumps({"k": decimal.Decimal(1.5)}, cls=DecimalEncoder)
    assert res_a == '{"k": 1}'
    assert res_b == '{"k": 2}'


def test_it_converts_sfn_datetimes_to_epoch():
    assert convert_iso8601_to_epoch("2020-01-06T16:12:57.092Z") == 1578327177
    assert convert_iso8601_to_epoch("2020-01-06T16:12:57Z") == 1578327177
    assert convert_iso8601_to_epoch("2020-01-06T16:12:57+00:00") == 1578327177
    assert convert_iso8601_to_epoch("2020-01-06T16:12:57.092+00:00") == 1578327177
    assert convert_iso8601_to_epoch("2020-01-06 16:12:57.092Z") == 1578327177
    assert convert_iso8601_to_epoch("2020-01-06 16:12:57Z") == 1578327177
    assert convert_iso8601_to_epoch("2020-01-06 16:12:57+00:00") == 1578327177
    assert convert_iso8601_to_epoch("2020-01-06 16:12:57.092+00:00") == 1578327177

    assert convert_iso8601_to_epoch("2020-01-06T16:12:57.092+01:00") == 1578323577
    assert convert_iso8601_to_epoch("2020-01-06T16:12:57+01:00") == 1578323577
    assert convert_iso8601_to_epoch("2020-01-06 16:12:57.092+01:00") == 1578323577
    assert convert_iso8601_to_epoch("2020-01-06 16:12:57+01:00") == 1578323577


def test_it_normalises_date_like_fields():
    assert {
        "a": [{"a": 1578327177, "b": "string"}],
        "b": [1578327177],
        "c": {"a": 1578327177},
        "d": 1578327177,
        "e": "string",
        "f": 2,
    } == normalise_dates(
        {
            "a": [{"a": "2020-01-06T16:12:57.092Z", "b": "string"}],
            "b": ["2020-01-06T16:12:57.092Z"],
            "c": {"a": "2020-01-06T16:12:57.092Z"},
            "d": "2020-01-06T16:12:57.092Z",
            "e": "string",
            "f": 2,
        }
    )


def test_it_deserializes_items():
    result = deserialize_item(
        {"DataMappers": {"L": [{"S": "test"}]}, "MatchId": {"S": "test"}}
    )

    assert {"MatchId": "test", "DataMappers": ["test"]} == result


@patch("boto_utils.table")
def test_it_returns_true_where_jobs_running(mock_table):
    mock_table.query.return_value = {"Items": [{}]}
    assert running_job_exists()
    mock_table.query.assert_called_with(
        IndexName=ANY,
        KeyConditionExpression=ANY,
        ScanIndexForward=False,
        FilterExpression="(#s = :r) or (#s = :q) or (#s = :c)",
        ExpressionAttributeNames={"#s": "JobStatus"},
        ExpressionAttributeValues={
            ":r": "RUNNING",
            ":q": "QUEUED",
            ":c": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
        },
        Limit=1,
    )


@patch("boto_utils.table")
def test_it_returns_true_where_jobs_not_running(mock_table):
    mock_table.query.return_value = {"Items": []}
    assert not running_job_exists()
    mock_table.query.assert_called_with(
        IndexName=ANY,
        KeyConditionExpression=ANY,
        ScanIndexForward=False,
        FilterExpression="(#s = :r) or (#s = :q) or (#s = :c)",
        ExpressionAttributeNames={"#s": "JobStatus"},
        ExpressionAttributeValues={
            ":r": "RUNNING",
            ":q": "QUEUED",
            ":c": "FORGET_COMPLETED_CLEANUP_IN_PROGRESS",
        },
        Limit=1,
    )


@patch("boto_utils.ssm")
def test_it_retrieves_config(mock_client):
    mock_client.get_parameter.return_value = {
        "Parameter": {
            "Value": json.dumps(
                {
                    "AthenaConcurrencyLimit": 1,
                    "AthenaQueryMaxRetries": 2,
                    "DeletionTasksMaxNumber": 1,
                    "QueryExecutionWaitSeconds": 1,
                    "QueryQueueWaitSeconds": 1,
                    "ForgetQueueWaitSeconds": 1,
                }
            )
        }
    }
    resp = get_config()

    assert {
        "AthenaConcurrencyLimit": 1,
        "AthenaQueryMaxRetries": 2,
        "DeletionTasksMaxNumber": 1,
        "QueryExecutionWaitSeconds": 1,
        "QueryQueueWaitSeconds": 1,
        "ForgetQueueWaitSeconds": 1,
    } == resp


@patch("boto_utils.ssm")
def test_it_handles_invalid_config(mock_client):
    mock_client.get_parameter.return_value = {"Parameter": {"Value": ""}}
    with pytest.raises(ValueError):
        get_config()


@patch("boto_utils.ssm")
def test_it_handles_config_not_found(mock_client):
    mock_client.get_parameter.side_effect = ClientError({}, "get_parameter")
    with pytest.raises(ClientError):
        get_config()


@patch("boto_utils.ssm")
def test_it_handles_other_config_errors(mock_client):
    mock_client.get_parameter.side_effect = RuntimeError("oops!")
    with pytest.raises(RuntimeError):
        get_config()


@patch("boto_utils.datetime")
def test_it_applies_time_delta(dt):
    dt.now.return_value = datetime.datetime(
        2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
    )
    assert 1580428800 == utc_timestamp(days=30)
    assert 1577836800 == utc_timestamp()


@patch("boto_utils.table")
def test_it_gets_job_expiry(table):
    get_job_expiry.cache_clear()
    table.get_item.return_value = {"Item": {"Expires": 123456}}
    assert 123456 == get_job_expiry("123")


@patch("boto_utils.table")
def test_it_returns_no_expiry(table):
    get_job_expiry.cache_clear()
    table.get_item.return_value = {"Item": {}}
    assert not get_job_expiry("123")


def test_it_parses_s3_url():
    assert ["bucket", "test/key"] == parse_s3_url("s3://bucket/test/key")
    assert ["bucket", "key"] == parse_s3_url("s3://bucket/key")
    assert ["bucket"] == parse_s3_url("s3://bucket")


def test_it_throws_for_invalid_urls():
    with pytest.raises(ValueError):
        parse_s3_url("not a url")
    with pytest.raises(ValueError):
        parse_s3_url(["s3://", "not", "string"])


def test_it_fetches_userinfo_from_lambda_event():
    result = get_user_info(
        {
            "requestContext": {
                "authorizer": {
                    "claims": {
                        "sub": "12345678-1234-1234-1234-123456123456",
                        "aud": "12345678901234567890123456",
                        "email_verified": "true",
                        "event_id": "12345678-1234-1234-1234-654321654321",
                        "token_use": "id",
                        "auth_time": "1581433394",
                        "iss": "https://cognito-idp.eu-west-1.amazonaws.com/eu-west-1_123456789",
                        "cognito:username": "email@test.com",
                        "exp": "Tue Feb 18 18:41:50 UTC 2020",
                        "iat": "Tue Feb 18 17:41:50 UTC 2020",
                        "email": "email@test.com",
                    }
                }
            }
        }
    )

    assert result == {
        "Username": "email@test.com",
        "Sub": "12345678-1234-1234-1234-123456123456",
    }


def test_it_fetches_userinfo_from_lambda_event_with_failover_in_place():
    result = get_user_info({"requestContext": {}})
    assert result == {"Username": "N/A", "Sub": "N/A"}


@patch("boto_utils.assume_role")
def test_it_returns_default_session(mock_assume_role):
    resp = get_session()
    mock_assume_role.assert_not_called()
    assert isinstance(resp, Session)


@patch("boto_utils.boto3")
@patch("boto_utils.assume_role")
def test_it_assumes_role_for_session_where_given(mock_assume_role, mock_boto):
    get_session(assume_role_arn="arn:aws:iam:accountid::role/rolename")
    mock_assume_role.assert_called_with(
        ANY, "arn:aws:iam:accountid::role/rolename", RoleSessionName=ANY
    )


@patch("boto_utils.s3")
def test_it_fetches_s3_manifest(mock_s3):
    mock_object = mock_get = mock_get_body = mock_read = mock_decode = MagicMock()
    mock_s3.Object.return_value = mock_object
    mock_object.get.return_value = mock_get
    mock_object.get.return_value = mock_get
    mock_get.get.return_value = mock_get_body
    mock_get_body.read.return_value = mock_read
    mock_read.decode.return_value = '{"hi": true}\n'
    bucket = "my-bucket"
    key = "manifests/job_id/data_mapper_id/manifest.json"
    result = fetch_job_manifest("s3://{}/{}".format(bucket, key))

    assert result == '{"hi": true}\n'
    mock_s3.Object.assert_called_once_with(bucket, key)
    mock_read.decode.assert_called_once_with("utf-8")


def test_it_iterates_over_json_lines():
    json_content = '{"hello":123,"world":true}\n{"hello":456,"world":false}\n'
    result = json_lines_iterator(json_content)
    assert isinstance(result, types.GeneratorType)
    assert list(result) == [
        {"hello": 123, "world": True},
        {"hello": 456, "world": False},
    ]


def test_it_iterates_over_json_lines_with_unparsed():
    json_content = '{"hello":123,"world":true}\n{"hello":456,"world":false}\n'
    result = json_lines_iterator(json_content, include_unparsed=True)
    assert isinstance(result, types.GeneratorType)
    parsed = []
    unparsed = []
    for line, unparsed_line in result:
        parsed.append(line)
        unparsed.append(unparsed_line)
    assert list(parsed) == [
        {"hello": 123, "world": True},
        {"hello": 456, "world": False},
    ]
    assert list(unparsed) == [
        '{"hello":123,"world":true}',
        '{"hello":456,"world":false}',
    ]


def test_it_raises_exception_for_invalid_json():
    json_content = '{"hello":123,"world":true}\nNOT_VALID\n'
    with pytest.raises(ValueError) as e:
        list(json_lines_iterator(json_content))
    assert e.value.args[0] == (
        "Serialization error when parsing JSON lines: "
        "Expecting value: line 2 column 1 (char 0)"
    )
