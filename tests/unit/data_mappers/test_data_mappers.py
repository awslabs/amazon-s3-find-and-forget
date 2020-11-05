import json
import os
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from mock import patch, ANY, Mock

with patch.dict(os.environ, {"DataMapperTable": "DataMapperTable"}):
    from backend.lambdas.data_mappers import handlers

pytestmark = [pytest.mark.unit, pytest.mark.api, pytest.mark.data_mappers]

autorization_mock = {
    "authorizer": {
        "claims": {"sub": "cognitoSub", "cognito:username": "cognitoUsername"}
    }
}


@patch("backend.lambdas.data_mappers.handlers.table")
def test_it_retrieves_all_items(table):
    """
    Test for all items : dict

    Args:
        table: (todo): write your description
    """
    table.scan.return_value = {"Items": []}
    response = handlers.get_data_mappers_handler({}, SimpleNamespace())
    assert {
        "statusCode": 200,
        "body": json.dumps({"DataMappers": [], "NextStart": None}),
        "headers": ANY,
    } == response
    table.scan.assert_called_with(Limit=10)


@patch("backend.lambdas.data_mappers.handlers.table")
def test_it_retrieves_all_items_with_size_and_pagination(table):
    """
    Test for pagination items in the pagination

    Args:
        table: (str): write your description
    """
    table.scan.return_value = {"Items": [{"DataMapperId": "foo"}]}
    response = handlers.get_data_mappers_handler(
        {"queryStringParameters": {"page_size": "1", "start_at": "bar"}},
        SimpleNamespace(),
    )
    assert {
        "statusCode": 200,
        "body": json.dumps(
            {"DataMappers": [{"DataMapperId": "foo"}], "NextStart": "foo"}
        ),
        "headers": ANY,
    } == response
    table.scan.assert_called_with(Limit=1, ExclusiveStartKey={"DataMapperId": "bar"})


@patch("backend.lambdas.data_mappers.handlers.table")
@patch("backend.lambdas.data_mappers.handlers.validate_mapper")
def test_it_creates_data_mapper(validate_mapper, table):
    """
    Mapper : func : ~. json / mapper

    Args:
        validate_mapper: (todo): write your description
        table: (str): write your description
    """
    response = handlers.put_data_mapper_handler(
        {
            "pathParameters": {"data_mapper_id": "test"},
            "body": json.dumps(
                {
                    "Columns": ["column"],
                    "QueryExecutor": "athena",
                    "QueryExecutorParameters": {
                        "DataCatalogProvider": "glue",
                        "Database": "test",
                        "Table": "test",
                    },
                    "Format": "parquet",
                    "RoleArn": "arn:aws:iam::accountid:role/S3F2DataAccessRole",
                    "DeleteOldVersions": False,
                }
            ),
            "requestContext": autorization_mock,
        },
        SimpleNamespace(),
    )

    assert 201 == response["statusCode"]
    assert {
        "DataMapperId": "test",
        "Columns": ["column"],
        "QueryExecutor": "athena",
        "QueryExecutorParameters": {
            "DataCatalogProvider": "glue",
            "Database": "test",
            "Table": "test",
        },
        "Format": "parquet",
        "CreatedBy": {"Username": "cognitoUsername", "Sub": "cognitoSub"},
        "RoleArn": "arn:aws:iam::accountid:role/S3F2DataAccessRole",
        "DeleteOldVersions": False,
    } == json.loads(response["body"])


@patch("backend.lambdas.data_mappers.handlers.table")
@patch("backend.lambdas.data_mappers.handlers.validate_mapper")
def test_it_modifies_data_mapper(validate_mapper, table):
    """
    Test if the mapper is valid

    Args:
        validate_mapper: (todo): write your description
        table: (str): write your description
    """
    def test_body(table_name):
        """
        Return the table as json.

        Args:
            table_name: (str): write your description
        """
        return json.dumps(
            {
                "Columns": ["column"],
                "QueryExecutor": "athena",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test",
                    "Table": table_name,
                },
                "Format": "parquet",
                "RoleArn": "arn:aws:iam::accountid:role/S3F2DataAccessRole",
                "DeleteOldVersions": False,
            }
        )

    create_response = handlers.put_data_mapper_handler(
        {
            "pathParameters": {"data_mapper_id": "test"},
            "body": test_body("test"),
            "requestContext": autorization_mock,
        },
        SimpleNamespace(),
    )

    edit_response = handlers.put_data_mapper_handler(
        {
            "pathParameters": {"data_mapper_id": "test"},
            "body": test_body("test1"),
            "requestContext": autorization_mock,
        },
        SimpleNamespace(),
    )

    assert 201 == edit_response["statusCode"]
    assert {
        "DataMapperId": "test",
        "Columns": ["column"],
        "QueryExecutor": "athena",
        "QueryExecutorParameters": {
            "DataCatalogProvider": "glue",
            "Database": "test",
            "Table": "test1",
        },
        "Format": "parquet",
        "CreatedBy": {"Username": "cognitoUsername", "Sub": "cognitoSub"},
        "RoleArn": "arn:aws:iam::accountid:role/S3F2DataAccessRole",
        "DeleteOldVersions": False,
    } == json.loads(edit_response["body"])


@patch("backend.lambdas.data_mappers.handlers.table")
@patch("backend.lambdas.data_mappers.handlers.validate_mapper")
def test_it_supports_optionals(validate_mapper, table):
    """
    Test if the test test_option for the test.

    Args:
        validate_mapper: (todo): write your description
        table: (str): write your description
    """
    response = handlers.put_data_mapper_handler(
        {
            "pathParameters": {"data_mapper_id": "test"},
            "body": json.dumps(
                {
                    "Columns": ["column"],
                    "QueryExecutor": "athena",
                    "QueryExecutorParameters": {
                        "DataCatalogProvider": "glue",
                        "Database": "test",
                        "Table": "test",
                    },
                    "RoleArn": "arn:aws:iam::accountid:role/S3F2DataAccessRole",
                }
            ),
            "requestContext": autorization_mock,
        },
        SimpleNamespace(),
    )

    assert 201 == response["statusCode"]
    assert {
        "DataMapperId": "test",
        "Columns": ["column"],
        "QueryExecutor": "athena",
        "QueryExecutorParameters": {
            "DataCatalogProvider": "glue",
            "Database": "test",
            "Table": "test",
        },
        "Format": "parquet",
        "DeleteOldVersions": True,
        "RoleArn": "arn:aws:iam::accountid:role/S3F2DataAccessRole",
        "CreatedBy": {"Username": "cognitoUsername", "Sub": "cognitoSub"},
    } == json.loads(response["body"])


@patch("backend.lambdas.data_mappers.handlers.validate_mapper")
def test_it_rejects_where_glue_validation_fails(validate_mapper):
    """
    Test if all tests in - side side side side side side.

    Args:
        validate_mapper: (todo): write your description
    """
    # Simulate raising an exception for table not existing
    validate_mapper.side_effect = ClientError(
        {"ResponseMetadata": {"HTTPStatusCode": 400}}, "get_table"
    )
    response = handlers.put_data_mapper_handler(
        {
            "pathParameters": {"data_mapper_id": "test"},
            "body": json.dumps(
                {
                    "Columns": ["column"],
                    "QueryExecutor": "athena",
                    "QueryExecutorParameters": {
                        "DataCatalogProvider": "glue",
                        "Database": "test",
                        "Table": "test",
                    },
                }
            ),
            "requestContext": autorization_mock,
        },
        SimpleNamespace(),
    )
    assert 400 == response["statusCode"]


@patch(
    "backend.lambdas.data_mappers.handlers.running_job_exists", Mock(return_value=False)
)
@patch("backend.lambdas.data_mappers.handlers.table")
def test_it_deletes_data_mapper(table):
    """
    Test for the given database exists.

    Args:
        table: (str): write your description
    """
    response = handlers.delete_data_mapper_handler(
        {"pathParameters": {"data_mapper_id": "test",}}, SimpleNamespace()
    )
    assert {"statusCode": 204, "headers": ANY} == response


@patch(
    "backend.lambdas.data_mappers.handlers.running_job_exists", Mock(return_value=True)
)
def test_it_rejects_deletes_whilst_job_running():
    """
    Rejects the test_it_mapper.

    Args:
    """
    response = handlers.delete_data_mapper_handler(
        {"pathParameters": {"data_mapper_id": "test",}}, SimpleNamespace()
    )
    assert {"body": ANY, "statusCode": 400, "headers": ANY} == response


@patch("backend.lambdas.data_mappers.handlers.get_existing_s3_locations")
@patch("backend.lambdas.data_mappers.handlers.get_table_details_from_mapper")
def test_it_rejects_non_existent_glue_tables(
    mock_get_details, get_existing_s3_locations
):
    """
    Test for non - non - non - exist.

    Args:
        mock_get_details: (todo): write your description
        get_existing_s3_locations: (todo): write your description
    """
    # Simulate raising an exception for table not existing
    get_existing_s3_locations.return_value = ["s3://bucket/prefix/"]
    mock_get_details.side_effect = ClientError(
        {"ResponseMetadata": {"HTTPStatusCode": 404}}, "get_table"
    )
    with pytest.raises(ClientError):
        handlers.validate_mapper(
            {
                "DataMapperId": "1234",
                "Columns": ["column"],
                "QueryExecutor": "athena",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test",
                    "Table": "test",
                },
            }
        )


@patch("backend.lambdas.data_mappers.handlers.get_existing_s3_locations")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_location")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_format")
@patch("backend.lambdas.data_mappers.handlers.get_table_details_from_mapper")
def test_it_rejects_overlapping_s3_paths(
    mock_get_details, mock_get_format, mock_get_location, get_existing_s3_locations
):
    """
    Rejects path paths to mock paths

    Args:
        mock_get_details: (todo): write your description
        mock_get_format: (todo): write your description
        mock_get_location: (todo): write your description
        get_existing_s3_locations: (todo): write your description
    """
    mock_get_details.return_value = get_table_stub({"Location": "s3://bucket/prefix/"})
    get_existing_s3_locations.return_value = ["s3://bucket/prefix/"]
    mock_get_location.return_value = "s3://bucket/prefix/"
    mock_get_format.return_value = (
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        {"serialization.format": "1"},
    )
    with pytest.raises(ValueError) as e:
        handlers.validate_mapper(
            {
                "DataMapperId": "1234",
                "Columns": ["column"],
                "QueryExecutor": "athena",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test",
                    "Table": "test",
                },
            }
        )
    assert (
        e.value.args[0] == "A data mapper already exists which covers this S3 location"
    )


@patch("backend.lambdas.data_mappers.handlers.get_existing_s3_locations")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_location")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_format")
@patch("backend.lambdas.data_mappers.handlers.get_table_details_from_mapper")
def test_it_rejects_not_supported_tables(
    mock_get_details, mock_get_format, mock_get_location, get_existing_s3_locations
):
    """
    Reject not_it configuration.

    Args:
        mock_get_details: (todo): write your description
        mock_get_format: (todo): write your description
        mock_get_location: (todo): write your description
        get_existing_s3_locations: (todo): write your description
    """
    mock_get_details.return_value = get_table_stub({"Location": "s3://bucket/prefix/"})
    get_existing_s3_locations.return_value = []
    mock_get_location.return_value = "s3://bucket/prefix/"
    mock_get_format.return_value = (
        "org.apache.hadoop.hive.serde2.OpenCSVSerde",
        {"field.delim": ","},
    )
    with pytest.raises(ValueError) as e:
        handlers.validate_mapper(
            {
                "DataMapperId": "1234",
                "Columns": ["column"],
                "QueryExecutor": "athena",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test",
                    "Table": "test",
                },
            }
        )
    assert (
        e.value.args[0] == "The format for the specified table is not supported. "
        "The SerDe lib must be one of org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe,"
        " org.apache.hive.hcatalog.data.JsonSerDe, org.openx.data.jsonserde.JsonSerDe"
    )


@patch("backend.lambdas.data_mappers.handlers.get_existing_s3_locations")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_location")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_format")
@patch("backend.lambdas.data_mappers.handlers.get_table_details_from_mapper")
def test_it_rejects_malformed_json(
    mock_get_details, mock_get_format, mock_get_location, get_existing_s3_locations
):
    """
    Rejects and_it_malformed_get_get_get_get_get_get_details

    Args:
        mock_get_details: (todo): write your description
        mock_get_format: (todo): write your description
        mock_get_location: (todo): write your description
        get_existing_s3_locations: (todo): write your description
    """
    mock_get_details.return_value = get_table_stub({"Location": "s3://bucket/prefix/"})
    get_existing_s3_locations.return_value = []
    mock_get_location.return_value = "s3://bucket/prefix/"
    mock_get_format.return_value = (
        "org.openx.data.jsonserde.JsonSerDe",
        {"ignore.malformed.json": "TRUE"},
    )
    with pytest.raises(ValueError) as e:
        handlers.validate_mapper(
            {
                "DataMapperId": "1234",
                "Columns": ["column"],
                "QueryExecutor": "athena",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test",
                    "Table": "test",
                },
            }
        )
    assert (
        e.value.args[0] == "The parameter ignore.malformed.json cannot be TRUE for "
        "SerDe library org.openx.data.jsonserde.JsonSerDe"
    )


@patch("backend.lambdas.data_mappers.handlers.get_existing_s3_locations")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_location")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_format")
@patch("backend.lambdas.data_mappers.handlers.get_table_details_from_mapper")
def test_it_rejects_json_with_dot_in_keys(
    mock_get_details, mock_get_format, mock_get_location, get_existing_s3_locations
):
    """
    Reject dot dot dot_json_with_details.

    Args:
        mock_get_details: (todo): write your description
        mock_get_format: (todo): write your description
        mock_get_location: (todo): write your description
        get_existing_s3_locations: (todo): write your description
    """
    mock_get_details.return_value = get_table_stub({"Location": "s3://bucket/prefix/"})
    get_existing_s3_locations.return_value = []
    mock_get_location.return_value = "s3://bucket/prefix/"
    mock_get_format.return_value = (
        "org.openx.data.jsonserde.JsonSerDe",
        {"dots.in.keys": "TRUE"},
    )
    with pytest.raises(ValueError) as e:
        handlers.validate_mapper(
            {
                "DataMapperId": "1234",
                "Columns": ["column"],
                "QueryExecutor": "athena",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test",
                    "Table": "test",
                },
            }
        )
    assert (
        e.value.args[0] == "The parameter dots.in.keys cannot be TRUE for "
        "SerDe library org.openx.data.jsonserde.JsonSerDe"
    )


@patch("backend.lambdas.data_mappers.handlers.get_existing_s3_locations")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_location")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_format")
@patch("backend.lambdas.data_mappers.handlers.get_table_details_from_mapper")
def test_it_rejects_json_with_column_mapping(
    mock_get_details, mock_get_format, mock_get_location, get_existing_s3_locations
):
    """
    Reject the mapping to json

    Args:
        mock_get_details: (todo): write your description
        mock_get_format: (todo): write your description
        mock_get_location: (todo): write your description
        get_existing_s3_locations: (todo): write your description
    """
    mock_get_details.return_value = get_table_stub({"Location": "s3://bucket/prefix/"})
    get_existing_s3_locations.return_value = []
    mock_get_location.return_value = "s3://bucket/prefix/"
    mock_get_format.return_value = (
        "org.openx.data.jsonserde.JsonSerDe",
        {"case.insensitive": "FALSE", "mapping.userid": "userId"},
    )
    with pytest.raises(ValueError) as e:
        handlers.validate_mapper(
            {
                "DataMapperId": "1234",
                "Columns": ["column"],
                "QueryExecutor": "athena",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "test",
                    "Table": "test",
                },
            }
        )
    assert (
        e.value.args[0] == "Column mappings are not supported for "
        "SerDe library org.openx.data.jsonserde.JsonSerDe"
    )


def test_it_detects_overlaps():
    """
    Detects overlap overlap overlap overlap overlap.

    Args:
    """
    assert handlers.is_overlap("s3://bucket/prefix/", "s3://bucket/prefix/subprefix/")
    assert handlers.is_overlap("s3://bucket/prefix/subprefix/", "s3://bucket/prefix/")


def test_it_detects_non_overlaps():
    """
    Test if all of the non - overlap.

    Args:
    """
    assert not handlers.is_overlap("s3://bucket/prefix/", "s3://otherbucket/prefix/")


def test_it_detects_non_overlapping_prefixes_in_same_bucket():
    """
    Test if any non - generated bucket meets meets any non - bucket.

    Args:
    """
    assert not handlers.is_overlap("s3://bucket/foo/bar", "s3://otherbucket/foo/baz")


@patch("backend.lambdas.data_mappers.handlers.table")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_location")
@patch("backend.lambdas.data_mappers.handlers.get_table_details_from_mapper")
def test_it_gets_existing_s3_locations(
    mock_get_details, mock_get_location, mock_dynamo
):
    """
    Get locations for locations that location.

    Args:
        mock_get_details: (todo): write your description
        mock_get_location: (todo): write your description
        mock_dynamo: (str): write your description
    """
    mock_dynamo.scan.return_value = {
        "Items": [
            {
                "DataMapperId": "1234",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "db",
                    "Table": "table",
                },
            }
        ]
    }
    mock_get_details.return_value = get_table_stub()
    mock_get_location.return_value = "s3://bucket/prefix/"
    resp = handlers.get_existing_s3_locations("2345")
    assert ["s3://bucket/prefix/"] == resp


@patch("backend.lambdas.data_mappers.handlers.table")
@patch("backend.lambdas.data_mappers.handlers.get_glue_table_location")
@patch("backend.lambdas.data_mappers.handlers.get_table_details_from_mapper")
def test_it_gets_existing_s3_locations_excluding_current_data_mapper_id(
    mock_get_details, mock_get_location, mock_dynamo
):
    """
    Test for existing locations that locations.

    Args:
        mock_get_details: (todo): write your description
        mock_get_location: (todo): write your description
        mock_dynamo: (str): write your description
    """
    mock_dynamo.scan.return_value = {
        "Items": [
            {
                "DataMapperId": "1234",
                "QueryExecutorParameters": {
                    "DataCatalogProvider": "glue",
                    "Database": "db",
                    "Table": "table",
                },
            }
        ]
    }
    mock_get_details.return_value = get_table_stub()
    mock_get_location.return_value = "s3://bucket/prefix/"
    resp = handlers.get_existing_s3_locations("1234")
    assert [] == resp


def test_it_gets_s3_location_for_glue_table():
    """
    Return the location of the location.

    Args:
    """
    resp = handlers.get_glue_table_location(get_table_stub())
    assert "s3://bucket/" == resp


def test_it_gets_glue_table_format_info():
    """
    Displays the glob table format.

    Args:
    """
    assert (
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        {"serialization.format": "1"},
    ) == handlers.get_glue_table_format(get_table_stub())


@patch("backend.lambdas.data_mappers.handlers.glue_client")
def test_it_gets_details_for_table(mock_client):
    """
    Return the details of the details of the mock.

    Args:
        mock_client: (todo): write your description
    """
    mock_client.get_table.return_value = get_table_stub()
    handlers.get_table_details_from_mapper(
        {
            "QueryExecutorParameters": {
                "DataCatalogProvider": "glue",
                "Database": "db",
                "Table": "table",
            }
        }
    )
    mock_client.get_table.assert_called_with(DatabaseName="db", Name="table")


def get_table_stub(storage_descriptor={}):
    """
    Returns the table statements.

    Args:
        storage_descriptor: (str): write your description
    """
    sd = {
        "Location": "s3://bucket/",
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"},
        },
    }
    sd.update(storage_descriptor)
    return {"Table": {"StorageDescriptor": sd}}
