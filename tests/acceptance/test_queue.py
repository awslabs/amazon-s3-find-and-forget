import pytest
from boto3.dynamodb.conditions import Key

pytestmark = [pytest.mark.acceptance, pytest.mark.queue]


def test_it_adds_to_queue(api_client, queue_base_endpoint, queue_table):
    # Arrange
    key = "test"
    config = {
        "MatchId": key,
        "Columns": ["a", "b"],
    }
    # Act
    response = api_client.patch(queue_base_endpoint, json=config)
    response_body = response.json()
    # Assert
    # Check the response is ok
    assert 201 == response.status_code
    assert config == response_body
    # Check the item exists in the DDB Table
    query_result = queue_table.query(KeyConditionExpression=Key("MatchId").eq(key))
    assert 1 == len(query_result["Items"])
    assert config == query_result["Items"][0]


def test_it_gets_queue(api_client, queue_base_endpoint, del_queue_item):
    # Arrange
    # Act
    response = api_client.get(queue_base_endpoint)
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert isinstance(response_body.get("MatchIds"), list)
    assert del_queue_item in response_body["MatchIds"]


def test_it_cancels_deletion(api_client, del_queue_item, queue_base_endpoint, queue_table):
    # Arrange
    key = del_queue_item["MatchId"]
    # Act
    response = api_client.delete("{}/matches/{}".format(queue_base_endpoint, key))
    # Assert
    assert 204 == response.status_code
    # Check the item doesn't exist in the DDB Table
    query_result = queue_table.query(KeyConditionExpression=Key("MatchId").eq(key))
    assert 0 == len(query_result["Items"])


@pytest.mark.skip
def test_it_processes_queue(api_client, del_queue_item, queue_base_endpoint, sf_client):
    # Arrange
    # Act
    response = api_client.delete(queue_base_endpoint)
    response_body = response.json()
    # Assert
    assert 204 == response.status_code
    # Check the execution started
    assert del_queue_item in response_body["JobId"]
    job = sf_client.describe_execution(
        executionArn=response_body["JobId"]
    )
    assert job["status"] in ["SUCCEEDED", "RUNNING"]
