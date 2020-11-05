import json
import os
from io import BytesIO
from types import SimpleNamespace

from mock import patch, MagicMock
import pytest
from botocore.exceptions import ClientError
from decorators import (
    with_logging,
    catch_errors,
    request_validator,
    add_cors_headers,
    s3_state_store,
    json_body_loader,
    sanitize_args,
    LogRecord,
)

pytestmark = [pytest.mark.unit, pytest.mark.layers]

test_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "TestSchema",
    "type": "object",
    "properties": {
        "pathParameters": {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
        },
    },
    "required": ["pathParameters"],
}


def test_it_validates_dict_keys():
    """
    Validate test keys.

    Args:
    """
    @request_validator(test_schema)
    def dummy_handler(event, context):
        """
        Returns a dummy handler.

        Args:
            event: (todo): write your description
            context: (dict): write your description
        """
        return {"statusCode": 200}

    resp = dummy_handler({"pathParameters": {"name": 123}}, SimpleNamespace())

    assert 422 == resp["statusCode"]
    assert "Message" in json.loads(resp["body"])


def test_it_returns_fatal_error_on_misconfiguration():
    """
    This function encoder returns json.

    Args:
    """
    @request_validator("not a schema")
    def dummy_handler(event, context):
        """
        Returns a dummy handler.

        Args:
            event: (todo): write your description
            context: (dict): write your description
        """
        return {"statusCode": 200}

    resp = dummy_handler({}, SimpleNamespace())

    assert 500 == resp["statusCode"]
    assert "Message" in json.loads(resp["body"])


def test_it_allows_valid_schemas():
    """
    Validate the test validation.

    Args:
    """
    @request_validator(test_schema)
    def dummy_handler(event, context):
        """
        Returns a dummy handler.

        Args:
            event: (todo): write your description
            context: (dict): write your description
        """
        return {"statusCode": 200}

    resp = dummy_handler({"pathParameters": {"name": "123"}}, SimpleNamespace())

    assert 200 == resp["statusCode"]


def test_it_catches_client_errors():
    """
    Raise client client client exceptions.

    Args:
    """
    @catch_errors
    def dummy_handler(event, context):
        """
        This method is used to create an event.

        Args:
            event: (todo): write your description
            context: (dict): write your description
        """
        raise ClientError(
            {"ResponseMetadata": {"HTTPStatusCode": 404}}, "some_operation"
        )

    resp = dummy_handler({}, SimpleNamespace())
    assert 404 == resp["statusCode"]
    assert "Message" in json.loads(resp["body"])


def test_it_catches_returnable_errors():
    """
    Return a json error.

    Args:
    """
    expected_msg = "A message we want the caller to see"

    @catch_errors
    def dummy_handler(event, context):
        """
        Raises an event.

        Args:
            event: (todo): write your description
            context: (dict): write your description
        """
        raise ValueError(expected_msg)

    resp = dummy_handler({}, SimpleNamespace())
    assert 400 == resp["statusCode"]
    body = json.loads(resp["body"])
    assert "Message" in body
    assert "Invalid request: {}".format(expected_msg) == body["Message"]


def test_it_catches_unhandled_errors():
    """
    Raise the error.

    Args:
    """
    @catch_errors
    def dummy_handler(event, context):
        """
        Emit an event handler.

        Args:
            event: (todo): write your description
            context: (dict): write your description
        """
        raise KeyError()

    resp = dummy_handler({}, SimpleNamespace())
    assert 400 == resp["statusCode"]
    assert "Message" in json.loads(resp["body"])


def test_it_wraps_with_logging():
    """
    Decorator for test test logging.

    Args:
    """
    with patch("decorators.logger") as logger:

        @with_logging
        def dummy_handler(event, context):
            """
            Dummy handler

            Args:
                event: (todo): write your description
                context: (dict): write your description
            """
            return "OK"

        ctx = SimpleNamespace()
        resp = dummy_handler({}, ctx)
        assert "OK" == resp
        logger.debug.assert_called()


@patch("os.getenv", MagicMock(return_value="https://site.com"))
def test_it_wraps_response_with_headers():
    """
    Generate a test response header.

    Args:
    """
    @add_cors_headers
    def dummy_handler(event, context):
        """
        Returns a dummy handler.

        Args:
            event: (todo): write your description
            context: (dict): write your description
        """
        return {"statusCode": 200}

    ctx = SimpleNamespace()
    resp = dummy_handler({}, ctx)

    assert resp["headers"] == {
        "Access-Control-Allow-Origin": "https://site.com",
        "Content-Type": "application/json",
    }


def test_it_disables_loading_offloading():
    """
    Decorator to specify a function that will be called on the minion.

    Args:
    """
    @s3_state_store(should_offload=False, offload_keys=["Not"])
    def my_func(event, *_):
        """
        Decorator to wrap a function.

        Args:
            event: (todo): write your description
            _: (todo): write your description
        """
        return event

    res = my_func({"Not": ["Offloaded"]}, {})
    assert {"Not": ["Offloaded"]} == res


def test_it_disables_loading_loading():
    """
    Decorator to specify a function that will be called on the minion.

    Args:
    """
    @s3_state_store(should_load=False, load_keys=["Data"])
    def my_func(event, *_):
        """
        Decorator to wrap a function.

        Args:
            event: (todo): write your description
            _: (todo): write your description
        """
        return event

    res = my_func({"Data": "s3://bucket/key"}, {})
    assert {"Data": "s3://bucket/key"} == res


@patch("decorators.s3")
@patch("decorators.uuid4", MagicMock(side_effect=["a", "b"]))
def test_it_offloads_state(mock_s3):
    """
    Test the state of the mock.

    Args:
        mock_s3: (todo): write your description
    """
    with patch.dict(os.environ, {"StateBucket": "bucket"}):

        @s3_state_store(offload_keys=["Dict", "List"], should_load=False)
        def my_func(event, *_):
            """
            Decorator to wrap a function.

            Args:
                event: (todo): write your description
                _: (todo): write your description
            """
            return event

        res = my_func(
            {"Dict": {"test": "data"}, "List": ["data"], "Not": ["Offloaded"]}, {}
        )
        assert {
            "Dict": "s3://bucket/state/a",
            "List": "s3://bucket/state/b",
            "Not": ["Offloaded"],
        } == res
        assert ("bucket", "state/a") == mock_s3.Object.call_args_list[0][0]
        assert ("bucket", "state/b") == mock_s3.Object.call_args_list[1][0]
        assert {"Body": '{"test": "data"}'} == mock_s3.Object().put.call_args_list[0][1]
        assert {"Body": '["data"]'} == mock_s3.Object().put.call_args_list[1][1]


@patch("decorators.s3")
@patch("decorators.uuid4", MagicMock(side_effect=["a", "b"]))
def test_it_offloads_nested_state(mock_s3):
    """
    Test if the state of the mock state.

    Args:
        mock_s3: (todo): write your description
    """
    with patch.dict(os.environ, {"StateBucket": "bucket"}):

        @s3_state_store(offload_keys=["Dict", "List"], should_load=False)
        def my_func(event, *_):
            """
            Decorator to wrap a function.

            Args:
                event: (todo): write your description
                _: (todo): write your description
            """
            return event

        res = my_func({"Data": {"Dict": {"test": "data"}, "List": ["data"],}}, {})
        assert {
            "Data": {"Dict": "s3://bucket/state/a", "List": "s3://bucket/state/b",}
        } == res
        assert ("bucket", "state/a") == mock_s3.Object.call_args_list[0][0]
        assert ("bucket", "state/b") == mock_s3.Object.call_args_list[1][0]
        assert {"Body": '{"test": "data"}'} == mock_s3.Object().put.call_args_list[0][1]
        assert {"Body": '["data"]'} == mock_s3.Object().put.call_args_list[1][1]


@patch("decorators.s3")
@patch("decorators.uuid4", MagicMock(side_effect=["a", "b"]))
def test_it_offloads_all_by_default(mock_s3):
    """
    Perform the state of the mock

    Args:
        mock_s3: (todo): write your description
    """
    with patch.dict(os.environ, {"StateBucket": "bucket"}):

        @s3_state_store(should_load=False)
        def my_func(event, *_):
            """
            Decorator to wrap a function.

            Args:
                event: (todo): write your description
                _: (todo): write your description
            """
            return event

        res = my_func({"Dict": {"test": "data"}, "List": ["data"],}, {})
        assert {"Dict": "s3://bucket/state/a", "List": "s3://bucket/state/b",} == res
        assert ("bucket", "state/a") == mock_s3.Object.call_args_list[0][0]
        assert ("bucket", "state/b") == mock_s3.Object.call_args_list[1][0]
        assert {"Body": '{"test": "data"}'} == mock_s3.Object().put.call_args_list[0][1]
        assert {"Body": '["data"]'} == mock_s3.Object().put.call_args_list[1][1]


def test_it_ignores_offloading_none_dict_events():
    """
    Return the current state of the scores that have been sent.

    Args:
    """
    with patch.dict(os.environ, {"StateBucket": "bucket"}):

        @s3_state_store(should_load=False)
        def my_func(event, *_):
            """
            Decorator to wrap a function.

            Args:
                event: (todo): write your description
                _: (todo): write your description
            """
            return event

        assert "string" == my_func("string", {})


@patch("decorators.s3")
@patch("decorators.uuid4", MagicMock(side_effect=["a"]))
def test_it_overrides_default_bucket_and_prefix(mock_s3):
    """
    Configure the bucket environment variables in the current bucket.

    Args:
        mock_s3: (todo): write your description
    """
    with patch.dict(os.environ, {"StateBucket": "bucket"}):

        @s3_state_store(
            offload_keys=["Dict"],
            should_load=False,
            prefix="custom/",
            bucket="otherbucket",
        )
        def my_func(event, *_):
            """
            Decorator to wrap a function.

            Args:
                event: (todo): write your description
                _: (todo): write your description
            """
            return event

        res = my_func({"Dict": {"test": "data"},}, {})
        assert {"Dict": "s3://otherbucket/custom/a",} == res
        assert ("otherbucket", "custom/a") == mock_s3.Object.call_args_list[0][0]
        assert {"Body": '{"test": "data"}'} == mock_s3.Object().put.call_args_list[0][1]


@patch("decorators.s3")
def test_it_loads_keys(mock_s3):
    """
    Test if the keys exist in the keys existance.

    Args:
        mock_s3: (todo): write your description
    """
    @s3_state_store(load_keys=["Dict", "List"], should_offload=False)
    def my_func(event, *_):
        """
        Decorator to wrap a function.

        Args:
            event: (todo): write your description
            _: (todo): write your description
        """
        return event

    mock_s3.Object().get.side_effect = [
        {"Body": BytesIO(b'{"test": "data"}')},
        {"Body": BytesIO(b'["data"]')},
    ]

    res = my_func(
        {
            "Dict": "s3://bucket/state/a",
            "List": "s3://bucket/state/b",
            "Not": "s3://bucket/state/c",
        },
        {},
    )
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
    """
    Reload all of the state of the state

    Args:
        mock_s3: (todo): write your description
    """
    @s3_state_store(load_keys=["Dict", "List"], should_offload=False)
    def my_func(event, *_):
        """
        Decorator to wrap a function.

        Args:
            event: (todo): write your description
            _: (todo): write your description
        """
        return event

    mock_s3.Object().get.side_effect = [
        {"Body": BytesIO(b'{"test": "data"}')},
        {"Body": BytesIO(b'["data"]')},
    ]

    res = my_func({"Dict": "s3://bucket/state/a", "List": "s3://bucket/state/b",}, {})
    assert {"Dict": {"test": "data"}, "List": ["data"],} == res
    # Start at call index 1 as Object already called during test setup
    assert ("bucket", "state/a") == mock_s3.Object.call_args_list[1][0]
    assert ("bucket", "state/b") == mock_s3.Object.call_args_list[2][0]


@patch("decorators.s3")
def test_it_loads_nested_state(mock_s3):
    """
    Test if the state of the test.

    Args:
        mock_s3: (todo): write your description
    """
    @s3_state_store(load_keys=["Dict", "List"], should_offload=False)
    def my_func(event, *_):
        """
        Decorator to wrap a function.

        Args:
            event: (todo): write your description
            _: (todo): write your description
        """
        return event

    mock_s3.Object().get.side_effect = [
        {"Body": BytesIO(b'{"test": "data"}')},
        {"Body": BytesIO(b'["data"]')},
    ]

    res = my_func(
        {"Data": {"Dict": "s3://bucket/state/a", "List": "s3://bucket/state/b",}}, {}
    )
    assert {"Data": {"Dict": {"test": "data"}, "List": ["data"],}} == res
    # Start at call index 1 as Object already called during test setup
    assert ("bucket", "state/a") == mock_s3.Object.call_args_list[1][0]
    assert ("bucket", "state/b") == mock_s3.Object.call_args_list[2][0]


def test_it_ignores_loading_none_dicts():
    """
    Return a dictionary of the scores that were sent to true.

    Args:
    """
    with patch.dict(os.environ, {"StateBucket": "bucket"}):

        @s3_state_store(should_offload=False)
        def my_func(event, *_):
            """
            Decorator to wrap a function.

            Args:
                event: (todo): write your description
                _: (todo): write your description
            """
            return event

        assert "string" == my_func("string", {})


def test_it_loads_json_body_event():
    """
    Evaluate the json body into a json.

    Args:
    """
    @json_body_loader
    def dummy_handler(event, context):
        """
        Dummy handler.

        Args:
            event: (todo): write your description
            context: (dict): write your description
        """
        return event

    expected = {"a": "payload"}
    loaded = dummy_handler({"body": json.dumps(expected)}, {})
    assert {"body": expected} == loaded


def test_it_ignores_non_str_body():
    """
    Return a json string that represents a non - rpc string.

    Args:
    """
    @json_body_loader
    def dummy_handler(event, context):
        """
        Dummy handler.

        Args:
            event: (todo): write your description
            context: (dict): write your description
        """
        return event

    loaded = dummy_handler({"body": 123}, {})
    assert {"body": 123} == loaded


def test_it_ignores_missing_body_key():
    """
    .. version of missing event ::

    Args:
    """
    @json_body_loader
    def dummy_handler(event, context):
        """
        Dummy handler.

        Args:
            event: (todo): write your description
            context: (dict): write your description
        """
        return event

    loaded = dummy_handler({"pathParameters": {"a": "b"}}, {})
    assert {"pathParameters": {"a": "b"}} == loaded


def test_it_sanitises_args():
    """
    Sanitizes the test_itit_args.

    Args:
    """
    # dicts
    assert {"MatchId": "*** MATCH ID ***"} == sanitize_args({"MatchId": "1234"})
    assert {"Arg": {"MatchId": "*** MATCH ID ***"}} == sanitize_args(
        {"Arg": {"MatchId": "1234"}}
    )
    # lists
    assert {"Matches": ["*** MATCH ID ***"]} == sanitize_args({"Matches": ["1234"]})
    assert [{"MatchId": "*** MATCH ID ***"}] == sanitize_args([{"MatchId": "1234"}])
    assert {"Arg": [{"MatchId": "*** MATCH ID ***"}]} == sanitize_args(
        {"Arg": [{"MatchId": "1234"}]}
    )
    # tuples
    assert ({"MatchId": "*** MATCH ID ***"}) == sanitize_args(({"MatchId": "1234"}))


def test_it_passes_through_none_sanitised_types():
    """
    Assertsures that the test args are none.

    Args:
    """
    assert sanitize_args(ValueError("A generic error"))
