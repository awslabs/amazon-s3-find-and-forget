from types import SimpleNamespace

import pytest
from mock import patch

from lambdas.src.tasks.query_deletion_queue import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


@patch("lambdas.src.tasks.query_deletion_queue.paginate")
def test_it_returns_all_results(paginate_mock):
    paginate_mock.return_value = iter([{"S": {"test": "result"}}])

    resp = handler({}, SimpleNamespace())
    assert [{"test": "result"}] == resp
