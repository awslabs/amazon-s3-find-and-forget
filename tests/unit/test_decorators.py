import json
import os
from io import BytesIO
from types import SimpleNamespace

from mock import patch, MagicMock
import pytest
from botocore.exceptions import ClientError
from decorators import with_logging, catch_errors, request_validator, add_cors_headers, s3_state_store, \
    json_body_loader, sanitize_args, LogRecord

pytestmark = [pytest.mark.unit, pytest.mark.layers]

test_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "TestSchema",
    "type": "object",
    "properties": {
        "pathParameters": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string"
                }
            },
            "required": ["name"]
        },
    },
    "required": ["pathParameters"]
}


def test_it_validates_dict_keys():
    @request_validator(test_schema)
    def dummy_handler(event, context):
        return {"statusCode": 200}

    resp = dummy_handler({
        "pathParameters": {
            "name": 123
        }
    }, SimpleNamespace())

    assert 422 == resp["statusCode"]
    assert "Message" in json.loads(resp["body"])


def test_it_returns_fatal_error_on_misconfiguration():
    @request_validator("not a schema")
    def dummy_handler(event, context):
        return {"statusCode": 200}

    resp = dummy_handler({}, SimpleNamespace())

    assert 500 == resp["statusCode"]
    assert "Message" in json.loads(resp["body"])


def test_it_allows_valid_schemas():
    @request_validator(test_schema)
    def dummy_handler(event, context):
        return {"statusCode": 200}

    resp = dummy_handler({
        "pathParameters": {
            "name": "123"
        }
    }, SimpleNamespace())

    assert 200 == resp["statusCode"]


def test_it_catches_client_errors():
    @catch_errors
    def dummy_handler(event, context):
        raise ClientError({"ResponseMetadata": {"HTTPStatusCode": 404}}, "some_operation")

    resp = dummy_handler({}, SimpleNamespace())
    assert 404 == resp["statusCode"]
    assert "Message" in json.loads(resp["body"])


def test_it_catches_returnable_errors():
    expected_msg = "A message we want the caller to see"
    @catch_errors
    def dummy_handler(event, context):
        raise ValueError(expected_msg)

    resp = dummy_handler({}, SimpleNamespace())
    assert 400 == resp["statusCode"]
    body = json.loads(resp["body"])
    assert "Message" in body
    assert "Invalid request: {}".format(expected_msg) == body["Message"]


def test_it_catches_unhandled_errors():
    @catch_errors
    def dummy_handler(event, context):
        raise KeyError()

    resp = dummy_handler({}, SimpleNamespace())
    assert 400 == resp["statusCode"]
    assert "Message" in json.loads(resp["body"])


def test_it_wraps_with_logging():
    with patch("decorators.logger") as logger:
        @with_logging
        def dummy_handler(event, context):
            return "OK"

        ctx = SimpleNamespace()
        resp = dummy_handler({}, ctx)
        assert "OK" == resp
        logger.debug.assert_called()


@patch("os.getenv", MagicMock(return_value='https://site.com'))
def test_it_wraps_response_with_headers():

    @add_cors_headers
    def dummy_handler(event, context):
        return {"statusCode": 200}

    ctx = SimpleNamespace()
    resp = dummy_handler({}, ctx)

    assert resp["headers"] == {
        'Access-Control-Allow-Origin': 'https://site.com',
        'Content-Type': 'application/json'
    }


def test_it_disables_loading_offloading():
    @s3_state_store(should_offload=False, offload_keys=["Not"])
    def my_func(event, *_):
        return event

    res = my_func({"Not": ["Offloaded"]}, {})
    assert {"Not": ["Offloaded"]} == res


def test_it_disables_loading_loading():
    @s3_state_store(should_load=False, load_keys=["Data"])
    def my_func(event, *_):
        return event

    res = my_func({"Data": "s3://bucket/key"}, {})
    assert {"Data": "s3://bucket/key"} == res


@patch("decorators.s3")
@patch("decorators.uuid4", MagicMock(side_effect=["a", "b"]))
def test_it_offloads_state(mock_s3):
    with patch.dict(os.environ, {"StateBucket": "bucket"}):
        @s3_state_store(offload_keys=["Dict", "List"], should_load=False)
        def my_func(event, *_):
            return event

        res = my_func({
            "Dict": {"test": "data"},
            "List": ["data"],
            "Not": ["Offloaded"]
        }, {})
        assert {
            "Dict": "s3://bucket/state/a",
            "List": "s3://bucket/state/b",
            "Not": ["Offloaded"]
        } == res
        assert ("bucket", "state/a") == mock_s3.Object.call_args_list[0][0]
        assert ("bucket", "state/b") == mock_s3.Object.call_args_list[1][0]
        assert {"Body": '{"test": "data"}'} == mock_s3.Object().put.call_args_list[0][1]
        assert {"Body": '["data"]'} == mock_s3.Object().put.call_args_list[1][1]


@patch("decorators.s3")
@patch("decorators.uuid4", MagicMock(side_effect=["a", "b"]))
def test_it_offloads_nested_state(mock_s3):
    with patch.dict(os.environ, {"StateBucket": "bucket"}):
        @s3_state_store(offload_keys=["Dict", "List"], should_load=False)
        def my_func(event, *_):
            return event

        res = my_func({
            "Data": {
                "Dict": {"test": "data"},
                "List": ["data"],
            }
        }, {})
        assert {
            "Data": {
                "Dict": "s3://bucket/state/a",
                "List": "s3://bucket/state/b",
            }
        } == res
        assert ("bucket", "state/a") == mock_s3.Object.call_args_list[0][0]
        assert ("bucket", "state/b") == mock_s3.Object.call_args_list[1][0]
        assert {"Body": '{"test": "data"}'} == mock_s3.Object().put.call_args_list[0][1]
        assert {"Body": '["data"]'} == mock_s3.Object().put.call_args_list[1][1]


@patch("decorators.s3")
@patch("decorators.uuid4", MagicMock(side_effect=["a", "b"]))
def test_it_offloads_all_by_default(mock_s3):
    with patch.dict(os.environ, {"StateBucket": "bucket"}):
        @s3_state_store(should_load=False)
        def my_func(event, *_):
            return event

        res = my_func({
            "Dict": {"test": "data"},
            "List": ["data"],
        }, {})
        assert {
            "Dict": "s3://bucket/state/a",
            "List": "s3://bucket/state/b",
        } == res
        assert ("bucket", "state/a") == mock_s3.Object.call_args_list[0][0]
        assert ("bucket", "state/b") == mock_s3.Object.call_args_list[1][0]
        assert {"Body": '{"test": "data"}'} == mock_s3.Object().put.call_args_list[0][1]
        assert {"Body": '["data"]'} == mock_s3.Object().put.call_args_list[1][1]


def test_it_ignores_offloading_none_dict_events():
    with patch.dict(os.environ, {"StateBucket": "bucket"}):
        @s3_state_store(should_load=False)
        def my_func(event, *_):
            return event

        assert "string" == my_func("string", {})


@patch("decorators.s3")
@patch("decorators.uuid4", MagicMock(side_effect=["a"]))
def test_it_overrides_default_bucket_and_prefix(mock_s3):
    with patch.dict(os.environ, {"StateBucket": "bucket"}):
        @s3_state_store(offload_keys=["Dict"], should_load=False, prefix="custom/", bucket="otherbucket")
        def my_func(event, *_):
            return event

        res = my_func({
            "Dict": {"test": "data"},
        }, {})
        assert {
            "Dict": "s3://otherbucket/custom/a",
        } == res
        assert ("otherbucket", "custom/a") == mock_s3.Object.call_args_list[0][0]
        assert {"Body": '{"test": "data"}'} == mock_s3.Object().put.call_args_list[0][1]


@patch("decorators.s3")
def test_it_loads_keys(mock_s3):
    @s3_state_store(load_keys=["Dict", "List"], should_offload=False)
    def my_func(event, *_):
        return event

    mock_s3.Object().get.side_effect = [
        {"Body": BytesIO(b'{"test": "data"}')},
        {"Body": BytesIO(b'["data"]')}
    ]

    res = my_func({
        "Dict": "s3://bucket/state/a",
        "List": "s3://bucket/state/b",
        "Not": "s3://bucket/state/c",
    }, {})
    assert {
        "Dict": {"test": "data"},
        "List": ["data"],
        "Not": "s3://bucket/state/c",
    } == res
    # Start at call index 1 as Object already called during test setup
    assert ("bucket", "state/a") == mock_s3.Object.call_args_list[1][0]
    assert ("bucket", "state/b") == mock_s3.Object.call_args_list[2][0]


@patch("decorators.s3")
def test_it_loads_all_by_default(mock_s3):
    @s3_state_store(load_keys=["Dict", "List"], should_offload=False)
    def my_func(event, *_):
        return event

    mock_s3.Object().get.side_effect = [
        {"Body": BytesIO(b'{"test": "data"}')},
        {"Body": BytesIO(b'["data"]')}
    ]

    res = my_func({
        "Dict": "s3://bucket/state/a",
        "List": "s3://bucket/state/b",
    }, {})
    assert {
        "Dict": {"test": "data"},
        "List": ["data"],
    } == res
    # Start at call index 1 as Object already called during test setup
    assert ("bucket", "state/a") == mock_s3.Object.call_args_list[1][0]
    assert ("bucket", "state/b") == mock_s3.Object.call_args_list[2][0]


@patch("decorators.s3")
def test_it_loads_nested_state(mock_s3):
    @s3_state_store(load_keys=["Dict", "List"], should_offload=False)
    def my_func(event, *_):
        return event

    mock_s3.Object().get.side_effect = [
        {"Body": BytesIO(b'{"test": "data"}')},
        {"Body": BytesIO(b'["data"]')}
    ]

    res = my_func({
        "Data": {
            "Dict": "s3://bucket/state/a",
            "List": "s3://bucket/state/b",
        }
    }, {})
    assert {
        "Data": {
            "Dict": {"test": "data"},
            "List": ["data"],
        }
    } == res
    # Start at call index 1 as Object already called during test setup
    assert ("bucket", "state/a") == mock_s3.Object.call_args_list[1][0]
    assert ("bucket", "state/b") == mock_s3.Object.call_args_list[2][0]


def test_it_ignores_loading_none_dicts():
    with patch.dict(os.environ, {"StateBucket": "bucket"}):
        @s3_state_store(should_offload=False)
        def my_func(event, *_):
            return event

        assert "string" == my_func("string", {})


def test_it_loads_json_body_event():
    @json_body_loader
    def dummy_handler(event, context):
        return event
    expected = {"a": "payload"}
    loaded = dummy_handler({"body": json.dumps(expected)}, {})
    assert {
        "body": expected
    } == loaded


def test_it_ignores_non_str_body():
    @json_body_loader
    def dummy_handler(event, context):
        return event
    loaded = dummy_handler({"body": 123}, {})
    assert {
        "body": 123
    } == loaded


def test_it_ignores_missing_body_key():
    @json_body_loader
    def dummy_handler(event, context):
        return event
    loaded = dummy_handler({"pathParameters": {"a": "b"}}, {})
    assert {"pathParameters": {"a": "b"}} == loaded


def test_it_sanitises_args():
    # dicts
    assert {"MatchId": "*** MATCH ID ***"} == sanitize_args({"MatchId": "1234"})
    assert {"Arg": {"MatchId": "*** MATCH ID ***"}} == sanitize_args({"Arg": {"MatchId": "1234"}})
    # lists
    assert {"Matches": ["*** MATCH ID ***"]} == sanitize_args({"Matches": ["1234"]})
    assert [{"MatchId": "*** MATCH ID ***"}] == sanitize_args([{"MatchId": "1234"}])
    assert {"Arg": [{"MatchId": "*** MATCH ID ***"}]} == sanitize_args({"Arg": [{"MatchId": "1234"}]})
    # tuples
    assert ({"MatchId": "*** MATCH ID ***"}) == sanitize_args(({"MatchId": "1234"}))


def test_it_passes_through_none_sanitised_types():
    assert sanitize_args(ValueError("A generic error"))
