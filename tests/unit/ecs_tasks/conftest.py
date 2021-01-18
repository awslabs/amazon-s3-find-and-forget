import json

import pytest


@pytest.fixture
def message_stub():
    def make_message(**kwargs):
        return json.dumps(
            {
                "JobId": "1234",
                "Object": "s3://bucket/path/basic.parquet",
                "Columns": [{"Column": "customer_id"}],
                "CompositeColumns": [],
                "DeleteOldVersions": False,
                "Format": "parquet",
                "Manifest": "s3://temp-bucket/manifests/1234/dm54321/manifest.json",
                **kwargs,
            }
        )

    return make_message
