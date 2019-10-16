from os import path

import pytest
import sys
from aws_xray_sdk import global_sdk_config
from aws_xray_sdk.core import xray_recorder


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("DeletionQueueTable", "TestDeletionQueue")


def pytest_configure(config):
    """
    Initial test env setup
    """
    global_sdk_config.set_sdk_enabled(False)
    sys.path.append(path.join("lambdas", "layers", "boto_factory", "python"))
    sys.path.append(path.join("lambdas", "layers", "decorators", "python"))


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
