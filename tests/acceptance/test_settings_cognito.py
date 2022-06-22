import pytest
from mock import ANY

pytestmark = [pytest.mark.acceptance_cognito, pytest.mark.api, pytest.mark.settings]


@pytest.mark.auth
def test_auth(api_client_cognito, settings_base_endpoint):
    assert (
        401
        == api_client_cognito.get(
            settings_base_endpoint, headers={"Authorization": None}
        ).status_code
    )


def test_it_gets_settings(api_client_cognito, settings_base_endpoint, stack):
    # Act
    response = api_client_cognito.get(settings_base_endpoint)
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert isinstance(response_body.get("Settings"), dict)
    assert response_body["Settings"] == {
        "AthenaConcurrencyLimit": ANY,
        "AthenaQueryMaxRetries": ANY,
        "DeletionTasksMaxNumber": ANY,
        "JobDetailsRetentionDays": ANY,
        "ForgetQueueWaitSeconds": ANY,
        "QueryExecutionWaitSeconds": ANY,
        "QueryQueueWaitSeconds": ANY,
    }
    assert (
        response.headers.get("Access-Control-Allow-Origin")
        == stack["APIAccessControlAllowOriginHeader"]
    )
