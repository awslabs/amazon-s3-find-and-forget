import time
import uuid

import mock
import pytest

pytestmark = [pytest.mark.acceptance, pytest.mark.api, pytest.mark.jobs, pytest.mark.usefixtures("empty_jobs")]


def test_it_gets_jobs(api_client, jobs_endpoint, job_factory, stack, execution_exists_waiter, sf_client):
    # Arrange
    job_id = job_factory()["Id"]
    execution_arn = "{}:{}".format(stack["StateMachineArn"].replace("stateMachine", "execution"), job_id)
    execution_exists_waiter.wait(executionArn=execution_arn)
    try:
        # Act
        response = api_client.get("{}/{}".format(jobs_endpoint, job_id))
        response_body = response.json()
        # Assert
        assert response.status_code == 200
        assert {
            "Id": job_id,
            "Sk": job_id,
            "Type": "Job",
            "JobStatus": mock.ANY,
            "GSIBucket": mock.ANY,
            "CreatedAt": mock.ANY,
            "AthenaConcurrencyLimit": mock.ANY,
            "DeletionTasksMaxNumber": mock.ANY,
            "WaitDurationQueryExecution": mock.ANY,
            "WaitDurationQueryQueue": mock.ANY,
            "WaitDurationForgetQueue": mock.ANY,
        } == response_body
        assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]
    finally:
        sf_client.stop_execution(executionArn=execution_arn)


def test_it_handles_unknown_jobs(api_client, jobs_endpoint, stack):
    # Arrange
    job_id = "invalid"
    # Act
    response = api_client.get("{}/{}".format(jobs_endpoint, job_id))
    # Assert
    assert response.status_code == 404
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_lists_jobs_by_date(api_client, jobs_endpoint, job_factory, stack, sf_client, execution_exists_waiter):
    # Arrange
    job_id_1 = job_factory(job_id=str(uuid.uuid4()), created_at=1576861489)["Id"]
    job_id_2 = job_factory(job_id=str(uuid.uuid4()), created_at=1576861490)["Id"]
    execution_arn_1 = "{}:{}".format(stack["StateMachineArn"].replace("stateMachine", "execution"), job_id_1)
    execution_arn_2 = "{}:{}".format(stack["StateMachineArn"].replace("stateMachine", "execution"), job_id_2)
    try:
        # Act
        response = api_client.get(jobs_endpoint)
        response_body = response.json()
        # Assert
        assert response.status_code == 200
        assert response_body["Jobs"][0]["CreatedAt"] >= response_body["Jobs"][1]["CreatedAt"]
        assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]
    finally:
        execution_exists_waiter.wait(executionArn=execution_arn_1)
        execution_exists_waiter.wait(executionArn=execution_arn_1)
        sf_client.stop_execution(executionArn=execution_arn_1)
        sf_client.stop_execution(executionArn=execution_arn_2)


def test_it_lists_job_events_by_date(api_client, jobs_endpoint, job_factory, stack, sf_client, execution_waiter):
    # Arrange
    job_id = str(uuid.uuid4())
    job_id = job_factory(job_id=job_id, created_at=1576861489)["Id"]
    execution_arn = "{}:{}".format(stack["StateMachineArn"].replace("stateMachine", "execution"), job_id)
    execution_waiter.wait(executionArn=execution_arn)
    try:
        # Act
        response = api_client.get("{}/{}/events".format(jobs_endpoint, job_id))
        response_body = response.json()
        # Assert
        assert response.status_code == 200
        assert 2 == len(response_body["JobEvents"])
        assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]
        assert response_body["JobEvents"][1]["CreatedAt"] >= response_body["JobEvents"][0]["CreatedAt"]
    finally:
        sf_client.stop_execution(executionArn=execution_arn)


def test_it_updates_job_in_response_to_events(job_factory, job_event_factory, job_table, stack, sf_client,
                                              job_finished_waiter):
    job_id = job_factory()["Id"]
    job_event_factory(job_id, "FindPhaseFailed", {})
    execution_arn = "{}:{}".format(stack["StateMachineArn"].replace("stateMachine", "execution"), job_id)
    job_finished_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
    try:
        item = job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]
        time.sleep(5)  # No item waiter so have to sleep
        assert "FIND_FAILED" == item["JobStatus"]
    finally:
        sf_client.stop_execution(executionArn=execution_arn)


def test_it_locks_job_status_for_failed_jobs(job_factory, job_event_factory, job_table, stack, sf_client,
                                             job_finished_waiter):
    job_id = job_factory(JobStatus="FAILED")["Id"]
    execution_arn = "{}:{}".format(stack["StateMachineArn"].replace("stateMachine", "execution"), job_id)
    job_finished_waiter.wait(TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}})
    try:
        job_event_factory(job_id, "JobSucceeded", {})
        item = job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]
        time.sleep(5)  # No item waiter so have to sleep
        assert "FAILED" == item["JobStatus"]
    finally:
        sf_client.stop_execution(executionArn=execution_arn)
