from types import SimpleNamespace

import pytest
from mock import patch

from backend.lambdas.tasks.scan_table import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.scan_table.paginate")
def test_it_returns_all_results(paginate_mock):
    expected = [{
      "DataMappers": {
        "L": [
          {
            "S": "test"
          }
        ]
      },
      "MatchId": {
        "S": "test"
      }
    }]
    paginate_mock.return_value = iter(expected)

    resp = handler({"TableName": "test"}, SimpleNamespace())
    assert {
       "Items": expected,
       "Count": 1
    } == resp
