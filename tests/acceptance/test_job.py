import mock
import pytest

pytestmark = [pytest.mark.acceptance, pytest.mark.jobs]


def test_it_gets_jobs(api_client, jobs_endpoint, state_machine, execution):
    # Arrange
    job_id = execution["executionArn"].rsplit(":", 1)[-1]
    # Act
    response = api_client.get("{}/{}".format(jobs_endpoint, job_id))
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert {
        "JobId": job_id,
        "Status": mock.ANY
    } == response_body
