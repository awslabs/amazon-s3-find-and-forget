from types import SimpleNamespace

import pytest
from mock import patch

from lambdas.src.tasks.get_query_results import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("lambdas.src.tasks.get_query_results.paginate")
def test_it_returns_only_paths(paginate_mock):
    paginate_mock.return_value = iter([
        {
            "Data": [
                {
                    "VarCharValue": "$path"
                },
            ]
        },
        {
            "Data": [
                {
                    "VarCharValue": "s3://mybucket/mykey1"
                },
            ]
        },
        {
            "Data": [
                {
                    "VarCharValue": "s3://mybucket/mykey2"
                },
            ]
        },
    ])

    resp = handler("123", SimpleNamespace())
    assert [
        "s3://mybucket/mykey1",
        "s3://mybucket/mykey2",
    ] == resp
