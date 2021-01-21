from mock import patch

import gzip
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import pandas as pd
import tempfile
from backend.ecs_tasks.delete_files.redaction_handler import (
    transform_json_rows,
    transform_parquet_rows,
)

pytestmark = [pytest.mark.unit, pytest.mark.ecs_tasks]


@patch("backend.ecs_tasks.delete_files.redaction_handler.get_config")
def test_it_generates_new_json_file_with_redacted_rows(mock_config):
    mock_config.return_value = {
        "test1": {
            "DeletionMode": "Redaction",
            "LogOriginalRows": False,
            "ColumnsToRedact": {"customer_id": "[REDACTED]"},
        }
    }
    found = [{"customer_id": "12345", "x": 1.2, "d": "2001-01-01"}]
    transformed = transform_json_rows(found, "test1")

    assert transformed == [{"customer_id": "[REDACTED]", "x": 1.2, "d": "2001-01-01"}]


@patch("backend.ecs_tasks.delete_files.redaction_handler.get_config")
def test_it_generates_new_json_file_with_redacted_rows_complex_columns(mock_config):
    mock_config.return_value = {
        "test1": {
            "DeletionMode": "Redaction",
            "LogOriginalRows": False,
            "ColumnsToRedact": {"customer.id": "REDACTED", "age": -1},
        }
    }
    found = [{"customer": {"id": "12345"}, "age": 33, "d": "2001-01-01"}]
    transformed = transform_json_rows(found, "test1")

    assert transformed == [
        {"customer": {"id": "REDACTED"}, "age": -1, "d": "2001-01-01"}
    ]


@patch("backend.ecs_tasks.delete_files.redaction_handler.get_config")
def test_it_generates_new_parquet_file_with_redacted_rows(mock_config):
    mock_config.return_value = {
        "test1": {
            "DeletionMode": "Redaction",
            "LogOriginalRows": False,
            "ColumnsToRedact": {"customer_id": "[REDACTED]"},
        }
    }
    found = {
        "customer_id": ["12345", "23456"],
        "x": [1.2, 3.4],
        "d": ["2001-01-01", "2001-01-02"],
    }
    transformed = transform_parquet_rows(found, "test1")

    assert transformed == {
        "customer_id": ["[REDACTED]", "[REDACTED]"],
        "x": [1.2, 3.4],
        "d": ["2001-01-01", "2001-01-02"],
    }
