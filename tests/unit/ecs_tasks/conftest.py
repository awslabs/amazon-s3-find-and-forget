import json

import pytest


@pytest.fixture
def message_stub():
    """
    Return a json formatted message.

    Args:
    """
    def make_message(**kwargs):
        """
        Make a message object.

        Args:
        """
        return json.dumps(
            {
                "JobId": "1234",
                "Object": "s3://bucket/path/basic.parquet",
                "Columns": [{"Column": "customer_id", "MatchIds": ["12345", "23456"]}],
                "DeleteOldVersions": False,
                "Format": "parquet",
                **kwargs,
            }
        )

    return make_message
