from botocore.exceptions import ClientError
from mock import patch, MagicMock, call

import pytest

from backend.ecs_tasks.delete_files.utils import retry_wrapper, remove_none

pytestmark = [pytest.mark.unit, pytest.mark.ecs_tasks]


def get_list_object_versions_error():
    """
    Get versions of versions versions.

    Args:
    """
    return ClientError(
        {
            "Error": {
                "Code": "InvalidArgument",
                "Message": "Invalid version id specified",
            }
        },
        "ListObjectVersions",
    )


@patch("time.sleep")
def test_it_doesnt_retry_success_fn(sleep_mock):
    """
    Test if retry for retry.

    Args:
        sleep_mock: (todo): write your description
    """
    fn = MagicMock()
    fn.side_effect = [31, 32]
    result = retry_wrapper(fn, retry_wait_seconds=1, retry_factor=3)(25)

    assert result == 31
    assert fn.call_args_list == [call(25)]
    assert not sleep_mock.called


@patch("time.sleep")
def test_it_retries_retriable_fn(sleep_mock):
    """
    Decorator for retrized retrized retries.

    Args:
        sleep_mock: (todo): write your description
    """
    fn = MagicMock()
    e = get_list_object_versions_error()
    fn.side_effect = [e, e, 32]
    result = retry_wrapper(fn, retry_wait_seconds=1, retry_factor=3)(22)

    assert result == 32
    assert fn.call_args_list == [call(22), call(22), call(22)]
    assert sleep_mock.call_args_list == [call(1), call(3)]


@patch("time.sleep")
def test_it_doesnt_retry_non_retriable_fn(sleep_mock):
    """
    Test if retry_retry_retry_retry_non_retry_retry.

    Args:
        sleep_mock: (todo): write your description
    """
    fn = MagicMock()
    fn.side_effect = NameError("fail!")

    with pytest.raises(NameError) as e:
        result = retry_wrapper(fn, retry_wait_seconds=1, retry_factor=3)(22)

    assert e.value.args[0] == "fail!"
    assert fn.call_args_list == [call(22)]
    assert not sleep_mock.called


@patch("time.sleep")
def test_it_retries_and_gives_up_fn(sleep_mock):
    """
    Decorator for gives and retries_and_fn.

    Args:
        sleep_mock: (todo): write your description
    """
    fn = MagicMock()
    fn.side_effect = get_list_object_versions_error()

    with pytest.raises(ClientError) as e:
        result = retry_wrapper(fn, max_retries=3)(22)

    assert (
        e.value.args[0]
        == "An error occurred (InvalidArgument) when calling the ListObjectVersions operation: Invalid version id specified"
    )
    assert fn.call_args_list == [call(22), call(22), call(22), call(22)]
    assert sleep_mock.call_args_list == [call(2), call(4), call(8)]


def test_it_removes_empty_keys():
    """
    Remove empty keys. empty. test.

    Args:
    """
    assert {"test": "value"} == remove_none({"test": "value", "none": None})
