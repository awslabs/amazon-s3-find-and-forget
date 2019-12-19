import pytest
from boto3.dynamodb.conditions import Key

pytestmark = [pytest.mark.acceptance, pytest.mark.api, pytest.mark.data_mappers]


def test_it_creates_data_mapper(api_client, data_mapper_base_endpoint, data_mapper_table, glue_table_factory, stack):
    # Arrange
    table = glue_table_factory()
    key = "test"
    data_mapper = {
        "DataMapperId": key,
        "Columns": ["a"],
        "QueryExecutor": "athena",
        "QueryExecutorParameters": {
            "DataCatalogProvider": "glue",
            "Database": table["Database"],
            "Table": table["Table"]
        },
        "Format": "parquet",
    }
    # Act
    response = api_client.put("{}/{}".format(data_mapper_base_endpoint, key), json=data_mapper)
    response_body = response.json()
    # Assert
    # Check the response is ok
    assert 201 == response.status_code
    assert data_mapper == response_body
    # Check the item exists in the DDB Table
    query_result = data_mapper_table.query(KeyConditionExpression=Key("DataMapperId").eq(key))
    assert 1 == len(query_result["Items"])
    assert data_mapper == query_result["Items"][0]
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_rejects_invalid_data_mapper(api_client, data_mapper_base_endpoint, stack):
    key = "test"
    response = api_client.put("{}/{}".format(data_mapper_base_endpoint, key), json={"INVALID": "PAYLOAD"})
    assert 422 == response.status_code
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_gets_all_data_mappers(api_client, data_mapper_base_endpoint, glue_data_mapper_factory, stack):
    # Arrange
    item = glue_data_mapper_factory()
    # Act
    response = api_client.get(data_mapper_base_endpoint)
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert isinstance(response_body.get("DataMappers"), list)
    assert item in response_body["DataMappers"]
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_deletes_data_mapper(api_client, glue_data_mapper_factory, data_mapper_base_endpoint, data_mapper_table, stack):
    # Arrange
    item = glue_data_mapper_factory()
    key = item["DataMapperId"]
    # Act
    response = api_client.delete("{}/{}".format(data_mapper_base_endpoint, key))
    # Assert
    assert 204 == response.status_code
    # Check the item doesn't exist in the DDB Table
    query_result = data_mapper_table.query(KeyConditionExpression=Key("DataMapperId").eq(key))
    assert 0 == len(query_result["Items"])
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]
