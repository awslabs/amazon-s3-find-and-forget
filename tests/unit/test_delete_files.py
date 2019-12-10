import os

from botocore.exceptions import ClientError
from mock import patch, MagicMock, mock_open, ANY
from types import SimpleNamespace

import json
import pyarrow.parquet as pq
import pytest
from pyarrow.lib import ArrowException

from backend.ecs_tasks.delete_files.delete_files import delete_and_write, execute, get_queue, get_container_id, \
    log_deletion, log_failed_deletion

pytestmark = [pytest.mark.unit]


@patch("time.sleep")
def test_it_sleeps_if_queue_empty(mock_sleep):
    mock_queue = MagicMock()
    mock_queue.receive_message.return_value = []
    mock_dlq = MagicMock()
    execute(mock_queue, SimpleNamespace(), mock_dlq)
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
    message_item.body = message_stub()
    mock_queue.receive_messages.return_value = [message_item]
    mock_dlq = MagicMock()
    mock_s3 = MagicMock()
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load_parquet.return_value = parquet_file

    execute(mock_queue, mock_s3, mock_dlq)
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
    stats = {"ProcessedRows": 0, "TotalRows": 3}
    with open("./tests/acceptance/data/basic.parquet", "rb") as f:
        parquet_file = pq.ParquetFile(f, memory_map=False)
        delete_and_write(parquet_file, 0, [column], mock_writer, stats)

    arrow_table = mock_writer.write_table.call_args[0][0].to_pandas().to_dict()
    assert len(arrow_table["customer_id"]) == 1
    assert arrow_table["customer_id"][0] == "34567"


@patch("boto3.resource")
@patch.dict(os.environ, {"AWS_DEFAULT_REGION": "eu-west-1"})
def test_sqs_using_vpc_compatible_endpoint(mock_resource):
    mock_queue = MagicMock()
    mock_resource.return_value = mock_queue
    mock_queue.Queue.return_value = MagicMock()

    get_queue("https://url/q.fifo")
    mock_resource.assert_called_with(
        service_name="sqs", endpoint_url="https://sqs.eu-west-1.amazonaws.com")
    mock_queue.Queue.assert_called_with("https://url/q.fifo")


@patch("backend.ecs_tasks.delete_files.delete_files.log_event")
@patch("backend.ecs_tasks.delete_files.delete_files.get_container_id")
def test_it_logs_deletions(mock_get_container, mock_log):
    mock_get_container.return_value = "4567"
    stats_stub = {"Some": "stats"}
    msg = json.loads(message_stub())
    log_deletion(msg, stats_stub)
    mock_log.assert_called_with(ANY, "1234-4567", "ObjectUpdated", {
        "Statistics": stats_stub,
        **msg
    })


@patch("backend.ecs_tasks.delete_files.delete_files.log_event")
@patch("backend.ecs_tasks.delete_files.delete_files.get_container_id")
def test_it_logs_failed_deletions(mock_get_container, mock_log):
    mock_get_container.return_value = "4567"
    msg = json.loads(message_stub())
    log_failed_deletion(msg, "Some error")
    mock_log.assert_called_with(ANY, "1234-4567", "ObjectUpdateFailed", {
        "Error": "Some error",
        "Message": msg
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


@patch("os.remove")
@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_and_write")
@patch("backend.ecs_tasks.delete_files.delete_files.log_failed_deletion")
def test_it_handles_missing_col_exceptions(mock_log, mock_load_parquet, mock_delete_write, mock_remove):
    # Arrange
    mock_delete_write.side_effect = KeyError()
    message_item = MagicMock()
    message_item.body = message_stub()
    mock_queue = MagicMock()
    mock_queue.receive_messages.return_value = [message_item]
    mock_dlq = MagicMock()
    mock_s3 = MagicMock()
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load_parquet.return_value = parquet_file
    # Act
    execute(mock_queue, mock_s3, mock_dlq)
    # Assert
    mock_remove.assert_called()
    mock_log.assert_called()
    mock_dlq.send_message.assert_called()


@patch("os.remove")
@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_and_write")
@patch("backend.ecs_tasks.delete_files.delete_files.log_failed_deletion")
def test_it_handles_arrow_exceptions(mock_log, mock_load_parquet, mock_delete_write, mock_remove):
    # Arrange
    mock_delete_write.side_effect = ArrowException()
    message_item = MagicMock()
    message_item.body = message_stub()
    mock_queue = MagicMock()
    mock_queue.receive_messages.return_value = [message_item]
    mock_dlq = MagicMock()
    mock_s3 = MagicMock()
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load_parquet.return_value = parquet_file
    # Act
    execute(mock_queue, mock_s3, mock_dlq)
    # Assert
    mock_remove.assert_called()
    mock_log.assert_called()
    mock_dlq.send_message.assert_called()


@patch("os.remove")
def test_it_validates_messages_with_missing_keys(mock_remove):
    # Arrange
    message_item = MagicMock()
    message_item.body = "{}"
    mock_queue = MagicMock()
    mock_queue.receive_messages.return_value = [message_item]
    mock_dlq = MagicMock()
    mock_s3 = MagicMock()
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    # Act
    execute(mock_queue, mock_s3, mock_dlq)
    # Assert
    mock_remove.assert_called()
    mock_dlq.send_message.assert_called()


@patch("os.remove")
def test_it_validates_messages_with_invalid_body(mock_remove):
    # Arrange
    message_item = MagicMock()
    message_item.body = "NOT JSON"
    mock_queue = MagicMock()
    mock_queue.receive_messages.return_value = [message_item]
    mock_dlq = MagicMock()
    mock_s3 = MagicMock()
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    # Act
    execute(mock_queue, mock_s3, mock_dlq)
    # Assert
    mock_remove.assert_called()
    mock_dlq.send_message.assert_called()


@patch("os.remove")
@patch("backend.ecs_tasks.delete_files.delete_files.log_failed_deletion")
def test_it_handles_s3_permission_issues(mock_log, mock_remove):
    tmp_file = "/tmp/new.parquet"
    mock_queue = MagicMock()
    message_item = MagicMock()
    message_item.body = message_stub()
    mock_queue.receive_messages.return_value = [message_item]
    mock_dlq = MagicMock()
    mock_s3 = MagicMock()
    mock_s3.open.side_effect = ClientError(operation_name="open", error_response={})

    execute(mock_queue, mock_s3, mock_dlq)
    mock_remove.assert_called_with(tmp_file)
    mock_log.assert_called()
    mock_dlq.send_message.assert_called()


def message_stub(**kwargs):
    return json.dumps({
        "JobId": "1234",
        "Object": "s3://bucket/path/basic.parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": ["12345", "23456"]}],
        **kwargs
    })
