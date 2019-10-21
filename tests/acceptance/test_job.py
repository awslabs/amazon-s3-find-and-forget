import pytest

pytestmark = [pytest.mark.acceptance, pytest.mark.jobs]


def test_it_gets_jobs(api_client, job_endpoint, job):
    # Arrange
    # Act
    response = api_client.get(job_endpoint + job["JobId"])
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert job == response_body
