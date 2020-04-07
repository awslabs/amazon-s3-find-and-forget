from io import BytesIO
from mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import pandas as pd
from backend.ecs_tasks.delete_files.parquet import delete_matches_from_file, delete_from_dataframe, load_parquet,\
    get_row_count

pytestmark = [pytest.mark.unit, pytest.mark.ecs_tasks]


@patch("backend.ecs_tasks.delete_files.parquet.delete_from_dataframe")
def test_it_generates_new_file_without_matches(mock_delete):
    # Arrange
    column = {"Column": "customer_id",
              "MatchIds": ["12345", "23456"]}
    data = [{'customer_id': '12345'}, {'customer_id': '23456'}]
    df = pd.DataFrame(data)
    buf = BytesIO()
    df.to_parquet(buf)
    br = pa.BufferReader(buf.getvalue())
    f = pq.ParquetFile(br, memory_map=False)
    mock_delete.return_value = pd.DataFrame([{'customer_id': '12345'}])
    # Act
    out, stats = delete_matches_from_file(f, [column])
    assert isinstance(out, pa.BufferOutputStream)
    assert {"ProcessedRows": 2, "DeletedRows": 1} == stats
    res = pa.BufferReader(out.getvalue())
    newf = pq.ParquetFile(res, memory_map=False)
    assert 1 == newf.read().num_rows


def test_delete_correct_rows_from_dataframe():
    data = [
        {'customer_id': '12345'},
        {'customer_id': '23456'},
        {'customer_id': '34567'},
    ]
    columns = [
        {"Column": "customer_id", "MatchIds": ["12345", "23456"]}
    ]
    df = pd.DataFrame(data)
    res = delete_from_dataframe(df, columns)
    assert len(res) == 1
    assert res["customer_id"].values[0] == "34567"


def test_it_gets_row_count():
    data = [
        {'customer_id': '12345'},
        {'customer_id': '23456'},
        {'customer_id': '34567'},
    ]
    df = pd.DataFrame(data)
    assert 3 == get_row_count(df)


def test_it_loads_parquet_files():
    data = [{'customer_id': '12345'}, {'customer_id': '23456'}]
    df = pd.DataFrame(data)
    buf = BytesIO()
    df.to_parquet(buf, compression="snappy")
    resp = load_parquet(buf)
    assert 2 == resp.read().num_rows
