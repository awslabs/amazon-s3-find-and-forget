import pytest
from mock import ANY

pytestmark = [pytest.mark.acceptance, pytest.mark.api, pytest.mark.settings]


@pytest.mark.auth
def test_auth(api_client, settings_base_endpoint):
    """
    Perform an auth.

    Args:
        api_client: (todo): write your description
        settings_base_endpoint: (bool): write your description
    """
    assert (
        401
        == api_client.get(
            settings_base_endpoint, headers={"Authorization": None}
        ).status_code
    )


def test_it_gets_settings(api_client, settings_base_endpoint, stack):
    """
    : param api_client

    Args:
        api_client: (todo): write your description
        settings_base_endpoint: (todo): write your description
        stack: (todo): write your description
    """
    # Act
    response = api_client.get(settings_base_endpoint)
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert isinstance(response_body.get("Settings"), dict)
    assert response_body["Settings"] == {
        "AthenaConcurrencyLimit": ANY,
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
