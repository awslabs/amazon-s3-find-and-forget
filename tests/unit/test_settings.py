import json
from types import SimpleNamespace

import pytest
from mock import patch

from backend.lambdas.settings import handlers

pytestmark = [pytest.mark.unit, pytest.mark.api, pytest.mark.settings]


@patch("backend.lambdas.settings.handlers.get_config")
def test_it_process_queue(mock_config):
    mock_config.return_value = {
        "AthenaConcurrencyLimit": 15,
        "DeletionTasksMaxNumber": 50,
        "QueryExecutionWaitSeconds": 5,
        "QueryQueueWaitSeconds": 5,
        "ForgetQueueWaitSeconds": 30,
    }
    response = handlers.list_settings_handler({}, SimpleNamespace())
    
    assert 200 == response["statusCode"]
    assert "headers" in response
    assert {
        "Settings": {
            "AthenaConcurrencyLimit": 15,
            "DeletionTasksMaxNumber": 50,
            "QueryExecutionWaitSeconds": 5,
            "QueryQueueWaitSeconds": 5,
            "ForgetQueueWaitSeconds": 30,
        }
    } == json.loads(response["body"])
