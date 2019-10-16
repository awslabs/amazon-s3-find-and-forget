import json
from types import SimpleNamespace

import pytest
from mock import patch

from lambdas.src import configuration

pytestmark = [pytest.mark.unit, pytest.mark.configuration, pytest.mark.skip]


@patch("lambdas.src.configuration.handlers.table")
def test_it_retrieves_all_items(table):
    table.scan.return_value = {"Items": []}
    response = configuration.handlers.retrieve_handler({}, SimpleNamespace())
    assert {
        "statusCode": 200,
        "body": json.dumps({"data": []})
    } == response


@patch("lambdas.src.configuration.handlers.table")
def test_it_creates_items(table):
    response = configuration.handlers.create_handler({
        "body": json.dumps({
            "S3Uri": "s3://blah",
            "Columns": ["user_id"],
            "ObjectTypes": ["parquet"],
            "S3Trigger": False,
        })
    }, SimpleNamespace())

    assert 200 == response["statusCode"]
    assert {
        "S3Uri": "s3://blah",
        "Columns": ["user_id"],
        "ObjectTypes": ["parquet"],
        "S3Trigger": False,
    } == json.loads(response["body"])


@patch("lambdas.src.configuration.handlers.table")
def test_it_provides_defaults(table):
    response = configuration.handlers.create_handler({
        "body": json.dumps({
            "S3Uri": "s3://test/path",
            "Columns": ["user_id"]
        })
    }, SimpleNamespace())

    assert 200 == response["statusCode"]
    assert {
        "S3Uri": "s3://test/path",
        "Columns": ["user_id"],
        "ObjectTypes": ["parquet"],
        "S3Trigger": True,
    } == json.loads(response["body"])


@patch("lambdas.src.configuration.handlers.table")
def test_it_deletes_items(table):
    response = configuration.handlers.delete_handler({
        "body": json.dumps({
            "S3Uri": "s3://test/path",
        })
    }, SimpleNamespace())
    assert {
        "statusCode": 204,
        "body": json.dumps({})
    } == response
