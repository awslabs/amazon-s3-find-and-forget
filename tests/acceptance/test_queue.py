import mock
import pytest
from boto3.dynamodb.conditions import Key

pytestmark = [pytest.mark.acceptance, pytest.mark.api, pytest.mark.queue, pytest.mark.usefixtures("empty_jobs")]


@pytest.mark.auth
def test_auth(api_client, queue_base_endpoint):
    headers = {"Authorization": None}
    assert 401 == api_client.patch(queue_base_endpoint, json={}, headers=headers).status_code
    assert 401 == api_client.get(queue_base_endpoint, headers=headers).status_code
    assert 401 == api_client.delete("{}/matches".format(queue_base_endpoint), json={}, headers=headers).status_code
    assert 401 == api_client.delete(queue_base_endpoint, headers=headers).status_code


def test_it_adds_to_queue(api_client, queue_base_endpoint, queue_table, stack):
    # Arrange
    key = "test"
    item = {
        "MatchId": key,
        "DataMappers": ["a", "b"],
    }
    expected = {
        "MatchId": key,
        "CreatedAt": mock.ANY,
        "DataMappers": ["a", "b"],
    }
    # Act
    response = api_client.patch(queue_base_endpoint, json=item)
    response_body = response.json()
    # Assert
    # Check the response is ok
    assert 201 == response.status_code
    assert expected == response_body
    # Check the item exists in the DDB Table
    query_result = queue_table.query(KeyConditionExpression=Key("MatchId").eq(key))
    assert 1 == len(query_result["Items"])
    assert expected == query_result["Items"][0]
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_rejects_invalid_add_to_queue(api_client, queue_base_endpoint, stack):
    response = api_client.patch(queue_base_endpoint, json={"INVALID": "PAYLOAD"})
    assert 422 == response.status_code
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


def test_it_rejects_invalid_deletion(api_client, del_queue_factory, queue_base_endpoint, queue_table, stack):
    # Arrange
    del_queue_item = del_queue_factory()
    match_id = del_queue_item["MatchId"]
    # Act
    response = api_client.delete("{}/matches".format(queue_base_endpoint), json={"Matches": [{
        "MatchId": match_id,
    }]})
    # Assert
    assert 422 == response.status_code
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_cancels_deletion(api_client, del_queue_factory, queue_base_endpoint, queue_table, stack):
    # Arrange
    del_queue_item = del_queue_factory()
    match_id = del_queue_item["MatchId"]
    # Act
    response = api_client.delete("{}/matches".format(queue_base_endpoint), json={"Matches": [{
        "MatchId": match_id,
        "CreatedAt": del_queue_item["CreatedAt"]
    }]})
    # Assert
    assert 204 == response.status_code
    # Check the item doesn't exist in the DDB Table
    query_result = queue_table.query(KeyConditionExpression=Key("MatchId").eq(match_id))
    assert 0 == len(query_result["Items"])
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_handles_not_found(api_client, del_queue_factory, queue_base_endpoint, queue_table, stack):
    # Arrange
    del_queue_item = del_queue_factory()
    match_id = del_queue_item["MatchId"]
    # Act
    response = api_client.delete("{}/matches".format(queue_base_endpoint), json={"Matches": [{
        "MatchId": match_id,
        "CreatedAt": del_queue_item["CreatedAt"]
    }]})
    # Assert
    assert 204 == response.status_code
    # Check the item doesn't exist in the DDB Table
    query_result = queue_table.query(KeyConditionExpression=Key("MatchId").eq(match_id))
    assert 0 == len(query_result["Items"])
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_disables_cancel_deletion_whilst_job_in_progress(api_client, queue_base_endpoint, sf_client, job_table, execution_exists_waiter,
                                                            job_finished_waiter, queue_table, del_queue_factory, stack):
    # Arrange
    del_queue_item = del_queue_factory()
    match_id = del_queue_item["MatchId"]
    response = api_client.delete(queue_base_endpoint)
    response_body = response.json()
    job_id = response_body["Id"]
    execution_arn = "{}:{}".format(stack["StateMachineArn"].replace("stateMachine", "execution"), job_id)
    # Act
    response = api_client.delete("{}/matches".format(queue_base_endpoint), json={"Matches": [{
        "MatchId": match_id,
        "CreatedAt": del_queue_item["CreatedAt"]
    }]})
    try:
        # Assert
        assert 400 == response.status_code
        # Check the item doesn't exist in the DDB Table
        query_result = queue_table.query(KeyConditionExpression=Key("MatchId").eq(match_id))
        assert 1 == len(query_result["Items"])
    finally:
        execution_exists_waiter.wait(executionArn=execution_arn)
        sf_client.stop_execution(executionArn=execution_arn)
        job_finished_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})


def test_it_processes_queue(api_client, queue_base_endpoint, sf_client, job_table, stack, job_complete_waiter,
                            config_mutator):
    # Arrange
    config_mutator(JobDetailsRetentionDays=0)
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
        job_complete_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
        query_result = job_table.query(KeyConditionExpression=Key("Id").eq(job_id) & Key("Sk").eq(job_id))
        assert 1 == len(query_result["Items"])
        assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]
    finally:
        sf_client.stop_execution(executionArn=execution_arn)


def test_it_sets_expiry(api_client, queue_base_endpoint, sf_client, job_table, stack, job_complete_waiter,
                        config_mutator):
    # Arrange
    config_mutator(JobDetailsRetentionDays=1)
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
        job_complete_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
        query_result = job_table.query(KeyConditionExpression=Key("Id").eq(job_id))
        assert len(query_result["Items"]) > 0
        assert all(["Expires" in i for i in query_result["Items"]])
        assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]
    finally:
        sf_client.stop_execution(executionArn=execution_arn)


def test_it_only_allows_one_concurrent_execution(api_client, queue_base_endpoint, sf_client, stack,
                                                 execution_exists_waiter):
    # Arrange
    # Start a job
    response = api_client.delete(queue_base_endpoint)
    response_body = response.json()
    job_id = response_body["Id"]
    execution_arn = "{}:{}".format(stack["StateMachineArn"].replace("stateMachine", "execution"), job_id)
    # Act
    # Start a second job
    response = api_client.delete(queue_base_endpoint)
    try:
        # Assert
        assert 400 == response.status_code
        assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]
    finally:
        execution_exists_waiter.wait(executionArn=execution_arn)
        sf_client.stop_execution(executionArn=execution_arn)
