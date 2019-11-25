from datetime import datetime
from mock import patch, MagicMock, mock
from types import SimpleNamespace

import json
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backend.ecs_tasks.delete_files.delete_files import delete_and_write, execute, get_queue

pytestmark = [pytest.mark.unit]


@patch("time.sleep")
def test_it_sleeps_if_queue_empty(mock_sleep):
    mock_queue = MagicMock()
    mock_queue.receive_message.return_value = []

    execute(mock_queue, SimpleNamespace())
    mock_sleep.assert_called_with(30)


@patch("os.remove")
@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_and_write")
def test_happy_path_when_queue_not_empty(mock_delete_and_write, mock_load_parquet, mock_pq_writer, mock_remove):
    object_path = "s3://bucket/path/basic.parquet"
    tmp_file = "/tmp/new.parquet"
    column = {"Column": "customer_id",
              "MatchIds": ["12345", "23456"]}
    mock_queue = MagicMock()
    message_item = MagicMock()
    message_item.body = json.dumps(
        {"Object": object_path, "Columns": [column]})
    mock_queue.receive_messages.return_value = [message_item]
    mock_s3 = MagicMock()
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load_parquet.return_value = parquet_file

    execute(mock_queue, mock_s3)
    mock_s3.open.assert_called_with(object_path, "rb")
    mock_delete_and_write.assert_called_with(
        mock.ANY, 0, [column], mock.ANY, mock.ANY)
    mock_s3.put.assert_called_with(tmp_file, object_path)
    mock_remove.assert_called_with(tmp_file)


@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter")
def test_delete_correct_rows_from_dataframe(mock_pq_writer):
    mock_writer = MagicMock()
    mock_pq_writer.return_value = mock_writer
    column = {"Column": "customer_id",
              "MatchIds": ["12345", "23456"]}
    stats = {"processed_rows": 0, "total_rows": 3}
    with open('./tests/acceptance/data/basic.parquet', "rb") as f:
        parquet_file = pq.ParquetFile(f, memory_map=False)
        delete_and_write(parquet_file, 0, [column], mock_writer, stats)

    arrow_table = mock_writer.write_table.call_args[0][0].to_pandas().to_dict()
    assert len(arrow_table['customer_id']) == 1
    assert arrow_table['customer_id'][0] == '34567'


@patch("boto3.resource")
@patch("os.getenv")
def test_sqs_using_vpc_compatible_endpoint(mock_getenv, mock_resource):
    mock_queue = MagicMock()
    mock_resource.return_value = mock_queue
    mock_queue.Queue.return_value = MagicMock()
    mock_getenv.side_effect = ["eu-west-1", "https://url/q.fifo"]

    q = get_queue()
    mock_resource.assert_called_with(
        service_name="sqs", endpoint_url="https://sqs.eu-west-1.amazonaws.com")
    mock_queue.Queue.assert_called_with("https://url/q.fifo")
