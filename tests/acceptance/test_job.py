import mock
import pytest

pytestmark = [pytest.mark.acceptance, pytest.mark.api, pytest.mark.jobs]


def test_it_gets_jobs(api_client, jobs_endpoint, execution, stack):
    # Arrange
    job_id = execution["executionArn"].rsplit(":", 1)[-1]
    # Act
    response = api_client.get("{}/{}".format(jobs_endpoint, job_id))
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert {
        "JobId": job_id,
        "JobStatus": mock.ANY,
        "StartTime": mock.ANY,
    } == response_body
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]


def test_it_handles_unknown_jobs(api_client, jobs_endpoint, stack):
    # Arrange
    job_id = "invalid"
    # Act
    response = api_client.get("{}/{}".format(jobs_endpoint, job_id))
    # Assert
    assert response.status_code == 404
    assert response.headers.get("Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]
