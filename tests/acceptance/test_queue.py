import pytest
from boto3.dynamodb.conditions import Key

pytestmark = [pytest.mark.acceptance, pytest.mark.api, pytest.mark.queue, pytest.mark.usefixtures("empty_jobs")]


def test_it_adds_to_queue(api_client, queue_base_endpoint, queue_table, stack):
    # Arrange
    key = "test"
    item = {
        "MatchId": key,
        "DataMappers": ["a", "b"],
    }
    # Act
    response = api_client.patch(queue_base_endpoint, json=item)
    response_body = response.json()
    # Assert
    # Check the response is ok
    assert 201 == response.status_code
    assert item == response_body
    # Check the item exists in the DDB Table
    query_result = queue_table.query(KeyConditionExpression=Key("MatchId").eq(key))
    assert 1 == len(query_result["Items"])
    assert item == query_result["Items"][0]
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_rejects_invalid_add_to_queue(api_client, queue_base_endpoint, stack):
    response = api_client.patch(queue_base_endpoint, json={"INVALID": "PAYLOAD"})
    assert 422 == response.status_code
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_rejects_duplicate_add_to_queue(api_client, queue_base_endpoint, del_queue_factory, stack):
    del_queue_item = del_queue_factory()
    response = api_client.patch(queue_base_endpoint, json={"MatchId": del_queue_item["MatchId"]})
    assert 400 == response.status_code
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_gets_queue(api_client, queue_base_endpoint, del_queue_factory, stack):
    # Arrange
    del_queue_item = del_queue_factory()
    # Act
    response = api_client.get(queue_base_endpoint)
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert isinstance(response_body.get("MatchIds"), list)
    assert del_queue_item in response_body["MatchIds"]
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_cancels_deletion(api_client, del_queue_factory, queue_base_endpoint, queue_table, stack):
    # Arrange
    del_queue_item = del_queue_factory()
    key = del_queue_item["MatchId"]
    # Act
    response = api_client.delete("{}/matches".format(queue_base_endpoint), json={"MatchIds": [key]})
    # Assert
    assert 204 == response.status_code
    # Check the item doesn't exist in the DDB Table
    query_result = queue_table.query(KeyConditionExpression=Key("MatchId").eq(key))
    assert 0 == len(query_result["Items"])
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_handles_not_found(api_client, del_queue_factory, queue_base_endpoint, queue_table, stack):
    # Arrange
    del_queue_item = del_queue_factory()
    key = del_queue_item["MatchId"]
    # Act
    response = api_client.delete("{}/matches".format(queue_base_endpoint), json={"MatchIds": [key]})
    # Assert
    assert 204 == response.status_code
    # Check the item doesn't exist in the DDB Table
    query_result = queue_table.query(KeyConditionExpression=Key("MatchId").eq(key))
    assert 0 == len(query_result["Items"])
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_processes_queue(api_client, queue_base_endpoint, sf_client, job_table, stack, execution_exists_waiter):
    # Arrange
    # Act
    response = api_client.delete(queue_base_endpoint)
    response_body = response.json()
    job_id = response_body["Id"]
    execution_arn = "{}:{}".format(stack["StateMachineArn"].replace("stateMachine", "execution"), job_id)
    try:
        # Assert
        assert 202 == response.status_code
        assert "Id" in response_body
        # Check the job was written to DynamoDB
        query_result = job_table.query(KeyConditionExpression=Key("Id").eq(job_id))
        assert 1 == len(query_result["Items"])
        # Verify the job started from the DynamoDB stream
        execution_exists_waiter.wait(executionArn=execution_arn)
        assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]
    finally:
        sf_client.stop_execution(executionArn=execution_arn)
