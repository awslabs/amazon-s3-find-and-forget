import time
from botocore.exceptions import ClientError


def remove_none(d: dict):
    return {k: v for k, v in d.items() if v is not None and v != ""}


def retry_wrapper(fn, retry_wait_seconds=2, retry_factor=2, max_retries=5):
    """Exponential back-off retry wrapper for ClientError exceptions"""

    def wrapper(*args, **kwargs):
        retry_current = 0
        last_error = None

        while retry_current <= max_retries:
            try:
                return fn(*args, **kwargs)
            except ClientError as e:
                nonlocal retry_wait_seconds
                if retry_current == max_retries:
                    break
                last_error = e
                retry_current += 1
                time.sleep(retry_wait_seconds)
                retry_wait_seconds *= retry_factor

        raise last_error

    return wrapper
