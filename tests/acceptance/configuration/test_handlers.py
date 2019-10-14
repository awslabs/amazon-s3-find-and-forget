import json
import os

from boto3.dynamodb.conditions import Key


def test_it_gets_configurations(lambda_client, index_config):
    # Arrange
    expected_body = {"data": [index_config]}
    # Act
    response = lambda_client.invoke(FunctionName="GetConfigurations")
    # Assert
    response_body = json.loads(response["Payload"].read())
    assert response_body["statusCode"] == 200
    assert json.loads(response_body["body"]) == expected_body


def test_it_creates_configuration(lambda_client, event_generator, dynamodb_resource):
    # Arrange
    key = "s3://bucket/path"
    config = {"s3_uri": key, "s3_trigger": True,
              "columns": ["user_id"], "object_types": ["parquet"]}
    api_gw_event = event_generator("apigateway", {"body": json.dumps(config)})

    # Act
    response = lambda_client.invoke(FunctionName="CreateConfiguration", Payload=json.dumps(api_gw_event))
    # Assert
    response_body = json.loads(response["Payload"].read())
    # Check the response is ok
    assert response_body["statusCode"] == 200
    assert json.loads(response_body["body"]) == config
    # Check the item exists in the DDB Table
    table = dynamodb_resource.Table(os.getenv("CONFIGURATION_TABLE_NAME", "TestConfiguration"))
    query_result = table.query(KeyConditionExpression=Key("S3Uri").eq(key))
    assert len(query_result["Items"]) == 1
    assert query_result["Items"][0] == config


def test_it_deletes_configuration(lambda_client, event_generator, dynamodb_resource, index_config):
    # Arrange
    key = index_config["S3Uri"]
    body = {"s3_uri": key}
    api_gw_event = event_generator("apigateway", {"body": json.dumps(body), "httpMethod": "DELETE"})
    # Act
    response = lambda_client.invoke(FunctionName="DeleteConfiguration", Payload=json.dumps(api_gw_event))
    # Assert
    response_body = json.loads(response["Payload"].read())
    assert response_body["statusCode"] == 204
    # Check the item doesn't exist in the DDB Table
    table = dynamodb_resource.Table(os.getenv("CONFIGURATION_TABLE_NAME", "TestConfiguration"))
    query_result = table.query(KeyConditionExpression=Key("S3Uri").eq(key))
    assert len(query_result["Items"]) == 0
