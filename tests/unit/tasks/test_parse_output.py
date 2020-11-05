import json
from types import SimpleNamespace

import pytest

from backend.lambdas.tasks.parse_output import handler

pytestmark = [pytest.mark.unit, pytest.mark.task]


def test_it_flattens_results():
    """
    Test for flattr : flattensens. flattrs.

    Args:
    """
    result = handler(json.dumps([{"Test": "Result"}]), SimpleNamespace())
    assert [{"Test": "Result"},] == result
