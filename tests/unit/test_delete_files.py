import os
from mock import patch, MagicMock, mock_open, ANY
from types import SimpleNamespace

import json
import pyarrow.parquet as pq
import pytest
from backend.ecs_tasks.delete_files.delete_files import delete_and_write, execute, get_queue, get_container_id, log_deletion

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
@patch("backend.ecs_tasks.delete_files.delete_files.log_deletion")
def test_happy_path_when_queue_not_empty(mock_log, mock_delete_and_write, mock_load_parquet, mock_pq_writer,
                                         mock_remove):
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
        ANY, 0, [column], ANY, ANY)
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


@patch("backend.ecs_tasks.delete_files.delete_files.log_event")
@patch("backend.ecs_tasks.delete_files.delete_files.get_container_id")
def test_it_logs_deletions(mock_get_container, mock_log):
    mock_get_container.return_value = "4567"
    message_stub = {"JobId": "1234", "Object": "s3://bucket/object/"}
    stats_stub = {"Some": "stats"}
    log_deletion(message_stub, stats_stub)
    mock_log.assert_called_with(ANY, "1234-4567", "ObjectUpdated", {
        "Statistics": stats_stub,
        **message_stub
    })


@patch("os.getenv", MagicMock(return_value="/some/path"))
@patch("os.path.isfile", MagicMock(return_value=True))
def test_it_loads_container_id_from_metadata():
    with patch("builtins.open", mock_open(read_data="{\"ContainerId\": \"123\"}")):
        resp = get_container_id()
        assert "123" == resp


@patch("backend.ecs_tasks.delete_files.delete_files.uuid4")
def test_it_provides_default_id(mock_uuid):
    mock_uuid.return_value = "123"
    resp = get_container_id()
    assert "123" == resp


def test_it_returns_uuid_as_string():
    resp = get_container_id()
    assert isinstance(resp, str)
