from io import BytesIO
from mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import pandas as pd
from backend.ecs_tasks.delete_files.parquet import (
    delete_matches_from_file,
    delete_from_table,
    load_parquet,
    get_row_count,
)

pytestmark = [pytest.mark.unit, pytest.mark.ecs_tasks]


@patch("backend.ecs_tasks.delete_files.parquet.delete_from_table")
def test_it_generates_new_file_without_matches(mock_delete):
    # Arrange
    column = {"Column": "customer_id", "MatchIds": ["12345", "23456"]}
    data = [{"customer_id": "12345"}, {"customer_id": "34567"}]
    df = pd.DataFrame(data)
    buf = BytesIO()
    df.to_parquet(buf)
    br = pa.BufferReader(buf.getvalue())
    f = pq.ParquetFile(br, memory_map=False)
    mock_df = pd.DataFrame([{"customer_id": "12345"}])
    mock_delete.return_value = [pa.Table.from_pandas(mock_df), 1]
    # Act
    out, stats = delete_matches_from_file(f, [column])
    assert isinstance(out, pa.BufferOutputStream)
    assert {"ProcessedRows": 2, "DeletedRows": 1} == stats
    res = pa.BufferReader(out.getvalue())
    newf = pq.ParquetFile(res, memory_map=False)
    assert 1 == newf.read().num_rows


def test_it_handles_files_with_multiple_row_groups_and_pandas_indexes():
    # Arrange
    data = [
        {"customer_id": "12345"},
        {"customer_id": "34567"},
    ]
    columns = [{"Column": "customer_id", "MatchIds": ["12345"]}]
    df = pd.DataFrame(data, list("ab"))
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    # Create parquet with multiple row groups
    with pq.ParquetWriter(buf, table.schema) as writer:
        for i in range(3):
            writer.write_table(table)
    br = pa.BufferReader(buf.getvalue())
    f = pq.ParquetFile(br, memory_map=False)
    # Act
    out, stats = delete_matches_from_file(f, columns)
    # Assert
    assert {"ProcessedRows": 6, "DeletedRows": 3} == stats
    res = pa.BufferReader(out.getvalue())
    newf = pq.ParquetFile(res, memory_map=False)
    assert 3 == newf.num_row_groups
    assert 3 == newf.read().num_rows


def test_delete_correct_rows_from_table():
    data = [
        {"customer_id": "12345"},
        {"customer_id": "23456"},
        {"customer_id": "34567"},
    ]
    columns = [{"Column": "customer_id", "MatchIds": ["12345", "23456"]}]
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    schema = pa.Schema.from_pandas(df)
    table, deleted_rows = delete_from_table(table, columns, schema)
    res = table.to_pandas()
    assert len(res) == 1
    assert deleted_rows == 2
    assert table.to_pydict() == {"customer_id": ["34567"]}


def test_it_handles_data_with_pandas_indexes():
    data = [
        {"customer_id": "12345"},
        {"customer_id": "23456"},
        {"customer_id": "34567"},
    ]
    columns = [{"Column": "customer_id", "MatchIds": ["12345", "23456"]}]
    df = pd.DataFrame(data, list("abc"))
    table = pa.Table.from_pandas(df)
    schema = pa.Schema.from_pandas(df)
    table, deleted_rows = delete_from_table(table, columns, schema)
    res = table.to_pandas()
    assert len(res) == 1
    assert deleted_rows == 2
    assert table.to_pydict() == {"customer_id": ["34567"], "__index_level_0__": ["c"]}


def test_delete_correct_rows_from_table_with_complex_types():
    data = {
        "customer_id": [12345, 23456, 34567],
        "user_info": [
            {"name": "matteo", "email": "12345@test.com"},
            {"name": "nick", "email": "23456@test.com"},
            {"name": "chris", "email": "34567@test.com"},
        ],
    }
    columns = [{"Column": "user_info.name", "MatchIds": ["matteo", "chris"]}]
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    schema = pa.Schema.from_pandas(df)
    table, deleted_rows = delete_from_table(table, columns, schema)
    res = table.to_pandas()
    assert len(res) == 1
    assert deleted_rows == 2
    assert res["customer_id"].values[0] == 23456
    # user_info is saved unflattened preserving original schema:
    assert res["user_info"].values[0] == {"name": "nick", "email": "23456@test.com"}


def test_it_gets_row_count():
    data = [
        {"customer_id": "12345"},
        {"customer_id": "23456"},
        {"customer_id": "34567"},
    ]
    df = pd.DataFrame(data)
    assert 3 == get_row_count(df)


def test_it_loads_parquet_files():
    data = [{"customer_id": "12345"}, {"customer_id": "23456"}]
    df = pd.DataFrame(data)
    buf = BytesIO()
    df.to_parquet(buf, compression="snappy")
    resp = load_parquet(buf)
    assert 2 == resp.read().num_rows
