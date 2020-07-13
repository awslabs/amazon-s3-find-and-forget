import json

import pytest


@pytest.fixture
def message_stub():
    def make_message(**kwargs):
        return json.dumps(
            {
                "JobId": "1234",
                "Object": "s3://bucket/path/basic.parquet",
                "Columns": [{"Column": "customer_id", "MatchIds": ["12345", "23456"]}],
                "DeleteOldVersions": False,
                **kwargs,
            }
        )

    return make_message
