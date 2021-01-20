from mock import patch

import gzip
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import pandas as pd
import tempfile
from backend.ecs_tasks.delete_files.redaction_handler import transform_rows

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
    transformed = transform_rows(found, "test1")

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
    transformed = transform_rows(found, "test1")

    assert transformed == [
        {"customer": {"id": "REDACTED"}, "age": -1, "d": "2001-01-01"}
    ]


def to_json_file(data, compressed=False):
    mode = "wb" if compressed else "w+t"
    tmp = tempfile.NamedTemporaryFile(mode=mode)
    tmp.write(data)
    tmp.flush()
    return open(tmp.name, "rb")


def to_compressed_json_file(data):
    return to_json_file(gzip.compress(bytes(data, "utf-8")), True)


def to_json_string(buf, compressed=False):
    tmp = tempfile.NamedTemporaryFile(mode="wb")
    tmp.write(buf.getvalue())
    tmp.flush()
    mode = "rb" if compressed else "r"
    result = open(tmp.name, mode)
    return result.read()


def to_decompressed_json_string(buf):
    return gzip.decompress(to_json_string(buf, True)).decode("utf-8")
