
import pytest
from mock import ANY

pytestmark = [pytest.mark.acceptance, pytest.mark.api, pytest.mark.settings]


def test_it_gets_settings(api_client, settings_base_endpoint, stack):
    # Act
    response = api_client.get(settings_base_endpoint)
    response_body = response.json()
    # Assert
    assert response.status_code == 200
    assert isinstance(response_body.get("Settings"), dict)
    assert response_body["Settings"] == {
        'AthenaConcurrencyLimit': ANY,
        'DeletionTasksMaxNumber': ANY,
        'DeletePreviousVersions': ANY,
        'JobDetailsRetentionDays': ANY,
        'ForgetQueueWaitSeconds': ANY,
        'QueryExecutionWaitSeconds': ANY,
        'QueryQueueWaitSeconds': ANY
    }
    assert response.headers.get(
        "Access-Control-Allow-Origin") == stack["APIAccessControlAllowOriginHeader"]
