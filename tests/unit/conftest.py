import sys
from os import path

import pytest
from aws_xray_sdk import global_sdk_config
from aws_xray_sdk.core import xray_recorder


def pytest_configure(config):
    """
    Initial test env setup
    """
    global_sdk_config.set_sdk_enabled(False)
    sys.path.append(path.join("backend", "lambda_layers", "boto_utils", "python"))
    sys.path.append(path.join("backend", "lambda_layers", "decorators", "python"))


@pytest.fixture(autouse=True)
def decorator_mocks(monkeypatch):
    """
    Mock the logging and tracing decorators
    """

    def mock_decorator(func):
        return func

    import decorators
    monkeypatch.setattr(decorators, "with_logger", mock_decorator)
    monkeypatch.setattr(xray_recorder, "capture", lambda _: mock_decorator)
