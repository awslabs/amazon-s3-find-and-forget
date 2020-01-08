import json
import os
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from mock import patch, ANY


with patch.dict(os.environ, {"DataMapperTable": "DataMapperTable"}):
    from backend.lambdas.data_mappers import handlers

pytestmark = [pytest.mark.unit, pytest.mark.api, pytest.mark.data_mappers]


@patch("backend.lambdas.data_mappers.handlers.table")
def test_it_retrieves_all_items(table):
    table.scan.return_value = {"Items": []}
    response = handlers.get_data_mappers_handler({}, SimpleNamespace())
    assert {
        "statusCode": 200,
        "body": json.dumps({"DataMappers": []}),
        "headers": ANY
    } == response


@patch("backend.lambdas.data_mappers.handlers.table")
@patch("backend.lambdas.data_mappers.handlers.validate_mapper")
def test_it_creates_data_mapper(validate_mapper, table):
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


@patch("backend.lambdas.data_mappers.handlers.table")
@patch("backend.lambdas.data_mappers.handlers.validate_mapper")
def test_it_provides_default_format(validate_mapper, table):
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


@patch("backend.lambdas.data_mappers.handlers.table")
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


@patch("backend.lambdas.data_mappers.handlers.validate_mapper")
def test_it_rejects_where_glue_validation_fails(validate_mapper):
    # Simulate raising an exception for table not existing
    validate_mapper.side_effect = ClientError({"ResponseMetadata": {"HTTPStatusCode": 400}}, "get_table")
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
    assert 400 == response["statusCode"]


@patch("backend.lambdas.data_mappers.handlers.table")
def test_it_deletes_data_mapper(table):
    response = handlers.delete_data_mapper_handler({
        "pathParameters": {
            "data_mapper_id": "test",
        }
    }, SimpleNamespace())
    assert {
        "statusCode": 204,
        "headers": ANY
    } == response


@patch("backend.lambdas.data_mappers.handlers.get_existing_s3_locations")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_location")
def test_it_rejects_non_existent_glue_tables(mock_get_location, get_existing_s3_locations):
    # Simulate raising an exception for table not existing
    get_existing_s3_locations.return_value = ["s3://bucket/prefix/"]
    mock_get_location.side_effect = ClientError({"ResponseMetadata": {"HTTPStatusCode": 404}}, "get_table")
    with pytest.raises(ClientError):
        handlers.validate_mapper({
            "Columns": ["column"],
            "QueryExecutor": "athena",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test",
                "Table": "test"
            },
        })


@patch("backend.lambdas.data_mappers.handlers.get_existing_s3_locations")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_location")
def test_it_rejects_overlapping_s3_paths(mock_get_location, get_existing_s3_locations):
    get_existing_s3_locations.return_value = ["s3://bucket/prefix/"]
    mock_get_location.return_value = "s3://bucket/prefix/"
    with pytest.raises(ValueError):
        handlers.validate_mapper({
            "Columns": ["column"],
            "QueryExecutor": "athena",
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "test",
                "Table": "test"
            },
        })


def test_it_detects_overlaps():
    assert handlers.is_overlap("s3://bucket/prefix/", "s3://bucket/prefix/subprefix/")
    assert handlers.is_overlap("s3://bucket/prefix/subprefix/", "s3://bucket/prefix/")


def test_it_detects_non_overlaps():
    assert not handlers.is_overlap("s3://bucket/prefix/", "s3://otherbucket/prefix/")


def test_it_detects_non_overlapping_prefixes_in_same_bucket():
    assert not handlers.is_overlap("s3://bucket/foo/bar", "s3://otherbucket/foo/baz")


@patch("backend.lambdas.data_mappers.handlers.glue_client")
def test_it_gets_s3_location_for_glue_table(mock_glue):
    mock_glue.get_table.return_value = get_table_stub()
    resp = handlers.get_glue_table_location("db", "table")
    assert "s3://bucket/" == resp


@patch("backend.lambdas.data_mappers.handlers.table")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_location")
def test_it_gets_existing_s3_locations(mock_get_location, mock_dynamo):
    mock_dynamo.scan.return_value = {
        "Items": [{
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "db",
                "Table": "table"
            }
        }]
    }
    mock_get_location.return_value = "s3://bucket/prefix/"
    resp = handlers.get_existing_s3_locations()
    assert [
        "s3://bucket/prefix/"
    ] == resp


def get_table_stub(s3_loc="s3://bucket/"):
    return {
        "Table": {
            "StorageDescriptor": {
                "Location": s3_loc
            }
        }
    }

