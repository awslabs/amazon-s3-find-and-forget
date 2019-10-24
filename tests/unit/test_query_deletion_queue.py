from types import SimpleNamespace

import pytest
from mock import patch

from lambdas.src.tasks.query_deletion_queue import handler, deserialize_item

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("lambdas.src.tasks.query_deletion_queue.paginate")
@patch("lambdas.src.tasks.query_deletion_queue.deserialize_item")
def test_it_returns_all_results(deserialize_item_mock, paginate_mock):
    paginate_mock.return_value = [{
      "Columns": {
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
    deserialize_item_mock.return_value = {"MatchId": "test", "Columns": ["test"]}

    resp = handler({}, SimpleNamespace())
    assert [{"MatchId": "test", "Columns": ["test"]}] == resp


def test_it_deserializes_items():
    result = deserialize_item({
      "Columns": {
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

    assert {"MatchId": "test", "Columns": ["test"]} == result
