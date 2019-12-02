import json
from types import SimpleNamespace

import mock
import pytest
from botocore.exceptions import ClientError

from decorators import with_logger, catch_errors, request_validator

pytestmark = [pytest.mark.unit, pytest.mark.layers]

test_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "TestSchema",
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
    },
    "required": ["name"]
}


def test_it_validates_dict_keys():
    @request_validator(test_schema, "pathParameters")
    def dummy_handler(event, context):
        return {"statusCode": 200}

    resp = dummy_handler({
        "pathParameters": {
            "name": 123
        }
    }, SimpleNamespace())

    assert 422 == resp["statusCode"]


def test_it_validates_str_keys():
    @request_validator(test_schema, "body")
    def dummy_handler(event, context):
        return {"statusCode": 200}

    resp = dummy_handler({
        "body": json.dumps({
            "name": 123
        })
    }, SimpleNamespace())

    assert 422 == resp["statusCode"]


def test_it_defaults_to_body_key():
    @request_validator(test_schema)
    def dummy_handler(event, context):
        return {"statusCode": 200}

    resp = dummy_handler({
        "body": json.dumps({
            "name": 123
        })
    }, SimpleNamespace())

    assert 422 == resp["statusCode"]


def test_it_returns_fatal_error_on_misconfiguration():
    @request_validator(test_schema, "non_existent")
    def dummy_handler(event, context):
        return {"statusCode": 200}

    resp = dummy_handler({}, SimpleNamespace())

    assert 500 == resp["statusCode"]


def test_it_allows_valid_schemas():
    @request_validator(test_schema)
    def dummy_handler(event, context):
        return {"statusCode": 200}

    resp = dummy_handler({
        "body": json.dumps({
            "name": "123"
        })
    }, SimpleNamespace())

    assert 200 == resp["statusCode"]


def test_it_catches_client_errors():
    @catch_errors
    def dummy_handler(event, context):
        raise ClientError({"ResponseMetadata": {"HTTPStatusCode": 404}}, "some_operation")

    resp = dummy_handler({}, SimpleNamespace())
    assert 404 == resp["statusCode"]


def test_it_catches_returnable_errors():
    expected_msg = "A message we want the caller to see"
    @catch_errors
    def dummy_handler(event, context):
        raise ValueError(expected_msg)

    resp = dummy_handler({}, SimpleNamespace())
    assert 400 == resp["statusCode"]
    assert expected_msg == resp["body"]


def test_it_catches_unhandled_errors():
    @catch_errors
    def dummy_handler(event, context):
        raise KeyError()

    resp = dummy_handler({}, SimpleNamespace())
    assert 400 == resp["statusCode"]


def test_it_wraps_with_logging():
    with mock.patch("decorators.logger"):
        @with_logger
        def dummy_handler(event, context):
            return "OK"

        ctx = SimpleNamespace()
        resp = dummy_handler({}, ctx)
        assert "OK" == resp
        assert hasattr(ctx, "logger")
