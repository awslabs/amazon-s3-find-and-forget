from types import SimpleNamespace

import pytest
from mock import patch

from lambdas.src.tasks.execute_query import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("lambdas.src.tasks.execute_query.client")
def test_it_executes_queries(client_mock):
    client_mock.start_query_execution.return_value = {
        "QueryExecutionId": "123"
    }

    resp = handler({"Query": "blah", "Bucket": "mybucket", "Prefix": "my_prefix"}, SimpleNamespace())
    assert "123" == resp
    client_mock.start_query_execution.assert_called_with(QueryString="blah", ResultConfiguration={
        'OutputLocation': 's3://mybucket/my_prefix/'
    })
