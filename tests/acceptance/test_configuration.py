import pytest
from boto3.dynamodb.conditions import Key

pytestmark = [pytest.mark.acceptance, pytest.mark.configuration, pytest.mark.skip]


def test_it_gets_configurations(api_client, configuration_endpoint, index_config):
    # Arrange
    # Act
    response = api_client.get(configuration_endpoint)
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert isinstance(response_body.get("data"), list)
    assert index_config in response_body["data"]


def test_it_creates_configuration(api_client, configuration_endpoint, config_table):
    # Arrange
    key = "s3://bucket/path"
    config = {"S3Uri": key, "S3Trigger": True,
              "Columns": ["user_id"], "ObjectTypes": ["parquet"]}
    # Act
    response = api_client.post(configuration_endpoint, json=config)
    response_body = response.json()
    # Assert
    # Check the response is ok
    assert 200 == response.status_code
    assert config == response_body
    # Check the item exists in the DDB Table
    query_result = config_table.query(KeyConditionExpression=Key("S3Uri").eq(key))
    assert 1 == len(query_result["Items"])
    assert config == query_result["Items"][0]


def test_it_deletes_configuration(api_client, index_config, configuration_endpoint, config_table):
    # Arrange
    key = index_config["S3Uri"]
    body = {"S3Uri": key}
    # Act
    response = api_client.delete(configuration_endpoint, json=body)
    # Assert
    assert 204 == response.status_code
    # Check the item doesn't exist in the DDB Table
    query_result = config_table.query(KeyConditionExpression=Key("S3Uri").eq(key))
    assert 0 == len(query_result["Items"])
