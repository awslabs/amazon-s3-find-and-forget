import json
import os
from types import SimpleNamespace

import pytest
from mock import patch


with patch.dict(os.environ, {"DataMapperTable": "DataMapperTable"}):
    from lambdas.src.data_mappers import handlers

pytestmark = [pytest.mark.unit, pytest.mark.data_mappers]


@patch("lambdas.src.data_mappers.handlers.table")
def test_it_retrieves_all_items(table):
    table.scan.return_value = {"Items": []}
    response = handlers.get_data_mappers_handler({}, SimpleNamespace())
    assert {
        "statusCode": 200,
        "body": json.dumps({"DataMappers": []})
    } == response


@patch("lambdas.src.data_mappers.handlers.table")
def test_it_creates_data_mapper(table):
    response = handlers.create_data_mapper_handler({
        "pathParameters": {
            "data_mapper_id": "test"
        },
        "body": json.dumps({
            "Columns": ["column"],
            "QueryExecutor": "athena",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test",
                "Table": "test"
            },
            "Format": "parquet"
        })
    }, SimpleNamespace())

    assert 201 == response["statusCode"]
    assert {
        "DataMapperId": "test",
        "Columns": ["column"],
        "QueryExecutor": "athena",
        "QueryExecutorParameters": {
            "DataCatalogProvider": "glue",
            "Database": "test",
            "Table": "test"
        },
        "Format": "parquet"
    } == json.loads(response["body"])


@patch("lambdas.src.data_mappers.handlers.table")
def test_it_provides_default_format(table):
    response = handlers.create_data_mapper_handler({
        "pathParameters": {
            "data_mapper_id": "test"
        },
        "body": json.dumps({
            "Columns": ["column"],
            "QueryExecutor": "athena",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test",
                "Table": "test"
            },
        })
    }, SimpleNamespace())

    assert 201 == response["statusCode"]
    assert {
        "DataMapperId": "test",
        "Columns": ["column"],
        "QueryExecutor": "athena",
        "QueryExecutorParameters": {
            "DataCatalogProvider": "glue",
            "Database": "test",
            "Table": "test"
        },
        "Format": "parquet"
    } == json.loads(response["body"])


@patch("lambdas.src.data_mappers.handlers.table")
def test_it_rejects_invalid_data_source(table):
    response = handlers.create_data_mapper_handler({
        "pathParameters": {
            "data_mapper_id": "test"
        },
        "body": json.dumps({
            "Columns": ["column"],
            "QueryExecutor": "unsupported",
            "QueryExecutorParameters": {},
        })
    }, SimpleNamespace())

    assert 422 == response["statusCode"]


@patch("lambdas.src.data_mappers.handlers.table")
def test_it_cancels_deletions(table):
    response = handlers.delete_data_mapper_handler({
        "pathParameters": {
            "data_mapper_id": "test",
        }
    }, SimpleNamespace())
    assert {
        "statusCode": 204
    } == response
