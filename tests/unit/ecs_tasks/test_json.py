from mock import patch

import gzip
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import pandas as pd
import tempfile
from backend.ecs_tasks.delete_files.json_handler import delete_matches_from_json_file

pytestmark = [pytest.mark.unit, pytest.mark.ecs_tasks]


def test_it_generates_new_json_file_without_matches():
    # Arrange
    to_delete = [{"Column": "customer_id", "MatchIds": ["23456"]}]
    data = (
        '{"customer_id": "12345", "x": 1.2, "d":"2001-01-01"}\n'
        '{"customer_id": "23456", "x": 2.3, "d":"2001-01-03"}\n'
        '{"customer_id": "34567", "x": 3.4, "d":"2001-01-05"}\n'
    )
    out_stream = to_json_file(data)
    # Act
    out, stats = delete_matches_from_json_file(out_stream, to_delete)
    assert isinstance(out, pa.BufferOutputStream)
    assert {"ProcessedRows": 3, "DeletedRows": 1} == stats
    assert to_json_string(out) == (
        '{"customer_id": "12345", "x": 1.2, "d":"2001-01-01"}\n'
        '{"customer_id": "34567", "x": 3.4, "d":"2001-01-05"}\n'
    )


def test_it_handles_json_with_gzip_compression():
    # Arrange
    to_delete = [{"Column": "customer_id", "MatchIds": ["23456"]}]
    data = (
        '{"customer_id": "12345", "x": 7, "d":"2001-01-01"}\n'
        '{"customer_id": "23456", "x": 8, "d":"2001-01-03"}\n'
        '{"customer_id": "34567", "x": 9, "d":"2001-01-05"}\n'
    )
    out_stream = to_compressed_json_file(data)
    # Act
    out, stats = delete_matches_from_json_file(out_stream, to_delete, True)
    assert isinstance(out, pa.BufferOutputStream)
    assert {"ProcessedRows": 3, "DeletedRows": 1} == stats
    assert to_decompressed_json_string(out) == (
        '{"customer_id": "12345", "x": 7, "d":"2001-01-01"}\n'
        '{"customer_id": "34567", "x": 9, "d":"2001-01-05"}\n'
    )


def test_delete_correct_rows_from_json_file_with_complex_types():
    # Arrange
    to_delete = [{"Column": "user.id", "MatchIds": ["23456"]}]
    data = (
        '{"user": {"id": "12345", "name": "John"}, "d":["2001-01-01"]}\n'
        '{"user": {"id": "23456", "name": "Jane"}, "d":[]}\n'
        '{"user": {"id": "34567", "name": "Mary"}, "d":["2001-01-08"]}\n'
    )
    out_stream = to_json_file(data)
    # Act
    out, stats = delete_matches_from_json_file(out_stream, to_delete)
    assert isinstance(out, pa.BufferOutputStream)
    assert {"ProcessedRows": 3, "DeletedRows": 1} == stats
    assert to_json_string(out) == (
        '{"user": {"id": "12345", "name": "John"}, "d":["2001-01-01"]}\n'
        '{"user": {"id": "34567", "name": "Mary"}, "d":["2001-01-08"]}\n'
    )


def test_delete_correct_rows_from_json_file_with_nullable_or_undefined_identifiers():
    # Arrange
    to_delete = [{"Column": "parents.mother", "MatchIds": ["23456"]}]
    data = (
        '{"user": {"id": "12345", "name": "John"}, "parents": {"mother": "23456"}}\n'
        '{"user": {"id": "23456", "name": "Jane"}, "parents": {"mother": null}}\n'
        '{"user": {"id": "34567", "name": "Mary"}}\n'
        '{"user": {"id": "45678", "name": "Mike"}, "parents": {}}\n'
        '{"user": {"id": "45678", "name": "Anna"}, "parents": null}\n'
    )
    out_stream = to_json_file(data)
    # Act
    out, stats = delete_matches_from_json_file(out_stream, to_delete)
    assert isinstance(out, pa.BufferOutputStream)
    assert {"ProcessedRows": 5, "DeletedRows": 1} == stats
    assert to_json_string(out) == (
        '{"user": {"id": "23456", "name": "Jane"}, "parents": {"mother": null}}\n'
        '{"user": {"id": "34567", "name": "Mary"}}\n'
        '{"user": {"id": "45678", "name": "Mike"}, "parents": {}}\n'
        '{"user": {"id": "45678", "name": "Anna"}, "parents": null}\n'
    )


def test_delete_correct_rows_from_json_file_with_multiple_identifiers():
    # Arrange
    to_delete = [
        {"Column": "user.id", "MatchIds": ["23456"]},
        {"Column": "mother", "MatchIds": ["23456"]},
    ]
    data = (
        '{"user": {"id": "12345", "name": "John"}, "mother": "23456"}\n'
        '{"user": {"id": "23456", "name": "Jane"}, "mother": null}\n'
        '{"user": {"id": "34567", "name": "Mary"}}\n'
    )
    out_stream = to_json_file(data)
    # Act
    out, stats = delete_matches_from_json_file(out_stream, to_delete)
    assert isinstance(out, pa.BufferOutputStream)
    assert {"ProcessedRows": 3, "DeletedRows": 2} == stats
    assert to_json_string(out) == '{"user": {"id": "34567", "name": "Mary"}}\n'


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
