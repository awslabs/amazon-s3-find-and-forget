from types import SimpleNamespace

import pytest
from mock import patch

from backend.lambdas.tasks.scan_table import handler, deserialize_item

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("backend.lambdas.tasks.scan_table.paginate")
@patch("backend.lambdas.tasks.scan_table.deserialize_item")
def test_it_returns_all_results(deserialize_item_mock, paginate_mock):
    paginate_mock.return_value = iter([{
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
    }])
    deserialize_item_mock.return_value = {"MatchId": "test", "DataMappers": ["test"]}

    resp = handler({"TableName": "test"}, SimpleNamespace())
    assert {
       "Items": [{"MatchId": "test", "DataMappers": ["test"]}],
       "Count": 1
    } == resp


def test_it_deserializes_items():
    result = deserialize_item({
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
    })

    assert {"MatchId": "test", "DataMappers": ["test"]} == result
