from io import BytesIO
from mock import patch

import pyarrow as pa
import pyarrow.json as pj
import pyarrow.parquet as pq
import pytest
import pandas as pd
import tempfile
from backend.ecs_tasks.delete_files.parquet_handler import (
    delete_matches_from_parquet_file,
    delete_from_table,
    load_parquet,
)

pytestmark = [pytest.mark.unit, pytest.mark.ecs_tasks]


@pytest.mark.only
@patch("backend.ecs_tasks.delete_files.parquet_handler.load_parquet")
@patch("backend.ecs_tasks.delete_files.parquet_handler.delete_from_table")
def test_it_generates_new_parquet_file_without_matches(mock_delete, mock_load_parquet):
    # Arrange
    column = {
        "Column": "customer_id",
        "MatchIds": ["12345", "23456"],
        "Type": "Simple",
    }
    data = [{"customer_id": "12345"}, {"customer_id": "34567"}]
    df = pd.DataFrame(data)
    buf = BytesIO()
    df.to_parquet(buf)
    br = pa.BufferReader(buf.getvalue())
    f = pq.ParquetFile(br, memory_map=False)
    mock_df = pd.DataFrame([{"customer_id": "12345"}])
    mock_delete.return_value = [pa.Table.from_pandas(mock_df), 1, 0]
    mock_load_parquet.return_value = f
    # Act
    out, stats = delete_matches_from_parquet_file(
        "input_file.parquet", column, "dm-123"
    )
    assert isinstance(out, pa.BufferOutputStream)
    assert {"ProcessedRows": 2, "DeletedRows": 1, "RedactedRows": 0} == stats
    res = pa.BufferReader(out.getvalue())
    newf = pq.ParquetFile(res, memory_map=False)
    assert 1 == newf.read().num_rows


@patch("backend.ecs_tasks.delete_files.parquet_handler.load_parquet")
def test_it_handles_files_with_multiple_row_groups_and_pandas_indexes(
    mock_load_parquet,
):
    # Arrange
    data = [
        {"customer_id": "12345"},
        {"customer_id": "34567"},
    ]
    columns = [{"Column": "customer_id", "MatchIds": ["12345"], "Type": "Simple"}]
    df = pd.DataFrame(data, list("ab"))
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    # Create parquet with multiple row groups
    with pq.ParquetWriter(buf, table.schema) as writer:
        for i in range(3):
            writer.write_table(table)
    br = pa.BufferReader(buf.getvalue())
    f = pq.ParquetFile(br, memory_map=False)
    mock_load_parquet.return_value = f
    # Act
    out, stats = delete_matches_from_parquet_file("input_file.parquet", columns)
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
    columns = [
        {"Column": "customer_id", "MatchIds": ["12345", "23456"], "Type": "Simple"}
    ]
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    table, deleted_rows = delete_from_table(table, columns)
    res = table.to_pandas()
    assert len(res) == 1
    assert deleted_rows == 2
    assert table.to_pydict() == {"customer_id": ["34567"]}


@pytest.mark.only
@patch("backend.ecs_tasks.delete_files.parquet_handler.transform_parquet_rows")
def test_redact_correct_rows_from_table(mock_transform_rows):
    mock_transform_rows.return_value = {"customer_id": ["REDACTED", "REDACTED"]}
    data = [
        {"customer_id": "12345"},
        {"customer_id": "23456"},
        {"customer_id": "34567"},
    ]
    columns = [
        {"Column": "customer_id", "MatchIds": ["12345", "23456"], "Type": "Simple"}
    ]
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    table, deleted_rows, redacted_rows = delete_from_table(table, columns, "test1")
    res = table.to_pandas()
    assert len(res) == 3
    assert deleted_rows == 0
    assert redacted_rows == 2
    assert table.to_pydict() == {"customer_id": ["34567", "REDACTED", "REDACTED"]}


def test_handles_lower_cased_column_names():
    data = [
        {"userData": {"customerId": "12345"}},
        {"userData": {"customerId": "23456"}},
        {"userData": {"customerId": "34567"}},
    ]
    columns = [
        {
            "Column": "userdata.customerid",
            "MatchIds": ["12345", "23456"],
            "Type": "Simple",
        }
    ]
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    table, deleted_rows = delete_from_table(table, columns)
    res = table.to_pandas()
    assert len(res) == 1
    assert deleted_rows == 2
    assert table.to_pydict() == {"userData": [{"customerId": "34567"}]}


def test_it_handles_data_with_pandas_indexes():
    data = [
        {"customer_id": "12345"},
        {"customer_id": "23456"},
        {"customer_id": "34567"},
    ]
    columns = [
        {"Column": "customer_id", "MatchIds": ["12345", "23456"], "Type": "Simple"}
    ]
    df = pd.DataFrame(data, list("abc"))
    table = pa.Table.from_pandas(df)
    table, deleted_rows = delete_from_table(table, columns)
    res = table.to_pandas()
    assert len(res) == 1
    assert deleted_rows == 2
    assert table.to_pydict() == {"customer_id": ["34567"], "__index_level_0__": ["c"]}


def test_delete_correct_rows_from_parquet_table_with_complex_types():
    data = {
        "customer_id": [12345, 23456, 34567],
        "user_info": [
            {"personal_information": {"name": "matteo", "email": "12345@test.com"}},
            {"personal_information": {"name": "nick", "email": "23456@test.com"}},
            {"personal_information": {"name": "chris", "email": "34567@test.com"}},
        ],
    }
    columns = [
        {
            "Column": "user_info.personal_information.name",
            "MatchIds": ["matteo", "chris"],
            "Type": "Simple",
        }
    ]
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    table, deleted_rows = delete_from_table(table, columns)
    res = table.to_pandas()
    assert len(res) == 1
    assert deleted_rows == 2
    assert res["customer_id"].values[0] == 23456
    # user_info is saved preserving original schema:
    assert res["user_info"].values[0] == {
        "personal_information": {"name": "nick", "email": "23456@test.com"}
    }


def test_delete_correct_rows_from_parquet_table_with_composite_types_tuple_col():
    data = {
        "customer_id": [12345, 23456, 34567],
        "first_name": ["john", "jane", "matteo"],
        "last_name": ["doe", "doe", "hey"],
    }
    columns = [
        {
            "Columns": ["first_name", "last_name"],
            "MatchIds": [["john", "doe"], ["jane", "doe"], ["matteo", "doe"]],
            "Type": "Composite",
        }
    ]
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    table, deleted_rows = delete_from_table(table, columns)
    res = table.to_pandas()
    assert len(res) == 1
    assert deleted_rows == 2
    assert res["customer_id"].values[0] == 34567


def test_delete_correct_rows_from_parquet_table_with_composite_types_single_col():
    data = {
        "customer_id": [12345, 23456, 34567],
        "first_name": ["john", "jane", "matteo"],
        "last_name": ["doe", "doe", "hey"],
    }
    columns = [{"Columns": ["last_name"], "MatchIds": [["doe"]], "Type": "Composite"}]
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    table, deleted_rows = delete_from_table(table, columns)
    res = table.to_pandas()
    assert len(res) == 1
    assert deleted_rows == 2
    assert res["customer_id"].values[0] == 34567


def test_delete_correct_rows_from_parquet_table_with_composite_types_multiple_types():
    data = {
        "age": [11, 12, 12],
        "customer_id": [12345, 23456, 34567],
        "first_name": ["john", "jane", "matteo"],
        "last_name": ["doe", "doe", "hey"],
    }
    columns = [
        {
            "Columns": ["age", "last_name"],
            "MatchIds": [[12, "doe"]],
            "Type": "Composite",
        }
    ]
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    table, deleted_rows = delete_from_table(table, columns)
    res = table.to_pandas()
    assert len(res) == 2
    assert deleted_rows == 1
    assert res["customer_id"].values[0] == 12345
    assert res["customer_id"].values[1] == 34567


def test_delete_correct_rows_from_parquet_table_with_complex_composite_types():
    data = {
        "customer_id": [12345, 23456, 34567],
        "details": [
            {"first_name": "John", "last_name": "Doe"},
            {"first_name": "Jane", "last_name": "Doe"},
            {"first_name": "Matteo", "last_name": "Hey"},
        ],
    }
    columns = [
        {
            "Columns": ["details.first_name", "details.last_name"],
            "MatchIds": [["John", "Doe"], ["Jane", "Doe"], ["Matteo", "Doe"]],
            "Type": "Composite",
        }
    ]
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    table, deleted_rows = delete_from_table(table, columns)
    res = table.to_pandas()
    assert len(res) == 1
    assert deleted_rows == 2
    assert res["customer_id"].values[0] == 34567


def test_delete_correct_rows_from_parquet_table_with_both_simple_and_composite_types():
    data = {
        "customer_id": [12345, 23456, 34567],
        "first_name": ["john", "jane", "matteo"],
        "last_name": ["doe", "doe", "hey"],
    }
    columns = [
        {"Column": "customer_id", "MatchIds": [12345], "Type": "Simple"},
        {
            "Columns": ["first_name", "last_name"],
            "MatchIds": [["jane", "doe"]],
            "Type": "Composite",
        },
    ]
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    table, deleted_rows = delete_from_table(table, columns)
    res = table.to_pandas()
    assert len(res) == 1
    assert deleted_rows == 2
    assert res["customer_id"].values[0] == 34567


def test_it_loads_parquet_files():
    data = [{"customer_id": "12345"}, {"customer_id": "23456"}]
    df = pd.DataFrame(data)
    buf = BytesIO()
    df.to_parquet(buf, compression="snappy")
    resp = load_parquet(buf)
    assert 2 == resp.read().num_rows
