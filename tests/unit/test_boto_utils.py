import types
import pytest
from mock import MagicMock

from boto_utils import paginate

pytestmark = [pytest.mark.unit, pytest.mark.layers]


def test_it_paginates():
    client = MagicMock()
    client.get_paginator.return_value = client
    client.some_method.__name__ = "some_method"
    client.paginate.return_value = iter([{
        "Test": [
            "val"
        ]
    }])
    result = paginate(client, client.some_method, ["Test"])
    assert isinstance(result, types.GeneratorType)
    assert ["val"] == list(result)


def test_it_supports_single_iter_key():
    client = MagicMock()
    client.get_paginator.return_value = client
    client.some_method.__name__ = "some_method"
    client.paginate.return_value = iter([{
        "Test": [
            "val"
        ]
    }])
    result = paginate(client, client.some_method, "Test")
    assert isinstance(result, types.GeneratorType)
    assert ["val"] == list(result)

