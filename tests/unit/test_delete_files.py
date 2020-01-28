import math
import os

from botocore.exceptions import ClientError
from mock import patch, MagicMock, mock_open, ANY

import json
import pyarrow.parquet as pq
import pytest
from pyarrow.lib import ArrowException

with patch.dict(os.environ, {
    "DELETE_OBJECTS_QUEUE": "https://url/q.fifo",
    "DLQ": "https://url/q"
}):
    from backend.ecs_tasks.delete_files.delete_files import delete_and_write, execute, get_container_id, \
        emit_deletion_event, emit_failed_deletion_event, check_file_size, get_max_file_size_bytes

pytestmark = [pytest.mark.unit]


@patch("os.path.exists", MagicMock(return_value=True))
@patch("os.remove")
@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_and_write")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.check_file_size", MagicMock())
def test_happy_path_when_queue_not_empty(mock_s3, mock_log, mock_delete_and_write, mock_load_parquet, mock_pq_writer,
                                         mock_remove):
    object_path = "s3://bucket/path/basic.parquet"
    tmp_file = "/tmp/new.parquet"
    column = {"Column": "customer_id",
              "MatchIds": ["12345", "23456"]}
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load_parquet.return_value = parquet_file

    def dw_side_effect(parquet_file, row_group, columns, writer, stats):
        stats["DeletedRows"] = 1

    mock_delete_and_write.side_effect = dw_side_effect
    execute(message_stub(), "receipt_handle")
    mock_s3.open.assert_called_with(object_path, "rb")
    mock_delete_and_write.assert_called_with(
        ANY, 0, [column], ANY, ANY)
    mock_s3.put.assert_called_with(tmp_file, object_path)
    mock_remove.assert_called_with(tmp_file)


@patch("os.path.exists", MagicMock(return_value=True))
@patch("os.remove")
@patch("backend.ecs_tasks.delete_files.delete_files.logger")
@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_and_write")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.check_file_size", MagicMock())
def test_warning_logged_for_no_deletions(
        mock_s3, mock_log, mock_delete_and_write, mock_load_parquet, mock_pq_writer, mock_logger, mock_remove):
    object_path = "s3://bucket/path/basic.parquet"
    tmp_file = "/tmp/new.parquet"
    column = {"Column": "customer_id",
              "MatchIds": ["12345", "23456"]}
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load_parquet.return_value = parquet_file

    execute(message_stub(), "receipt_handle")
    mock_s3.open.assert_called_with(object_path, "rb")
    mock_delete_and_write.assert_called_with(
        ANY, 0, [column], ANY, ANY)
    mock_s3.put.assert_not_called()
    mock_logger.warning.assert_called()
    mock_remove.assert_called_with(tmp_file)


@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter")
def test_delete_correct_rows_from_dataframe(mock_pq_writer):
    mock_writer = MagicMock()
    mock_pq_writer.return_value = mock_writer
    column = {"Column": "customer_id",
              "MatchIds": ["12345", "23456"]}
    stats = {"ProcessedRows": 0, "TotalRows": 3, "DeletedRows": 0}
    with open("./tests/acceptance/data/basic.parquet", "rb") as f:
        parquet_file = pq.ParquetFile(f, memory_map=False)
        delete_and_write(parquet_file, 0, [column], mock_writer, stats)

    arrow_table = mock_writer.write_table.call_args[0][0].to_pandas().to_dict()
    assert len(arrow_table["customer_id"]) == 1
    assert arrow_table["customer_id"][0] == "34567"


@patch("backend.ecs_tasks.delete_files.delete_files.emit_event")
@patch("backend.ecs_tasks.delete_files.delete_files.get_container_id")
def test_it_emits_deletions(mock_get_container, mock_emit):
    mock_get_container.return_value = "4567"
    stats_stub = {"Some": "stats"}
    msg = json.loads(message_stub())
    emit_deletion_event(msg, stats_stub)
    mock_emit.assert_called_with("1234", "ObjectUpdated", {
        "Statistics": stats_stub,
        "Object": "s3://bucket/path/basic.parquet",
    }, 'Task_4567')


@patch("backend.ecs_tasks.delete_files.delete_files.emit_event")
@patch("backend.ecs_tasks.delete_files.delete_files.get_container_id")
def test_it_emits_failed_deletions(mock_get_container, mock_emit):
    mock_get_container.return_value = "4567"
    msg = json.loads(message_stub())
    emit_failed_deletion_event(msg, "Some error")
    mock_emit.assert_called_with("1234", "ObjectUpdateFailed", {
        "Error": "Some error",
        "Message": msg
    }, 'Task_4567')


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


@patch("os.path.exists", MagicMock(return_value=True))
@patch("os.remove")
@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_and_write")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.s3", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
@patch("backend.ecs_tasks.delete_files.delete_files.dlq")
@patch("backend.ecs_tasks.delete_files.delete_files.check_file_size", MagicMock())
def test_it_handles_missing_col_exceptions(mock_dlq, mock_queue, mock_log, mock_load_parquet, mock_delete_write,
                                           mock_remove):
    # Arrange
    mock_delete_write.side_effect = KeyError("FAIL")
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load_parquet.return_value = parquet_file
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_remove.assert_called()
    mock_log.assert_called_with(ANY, "Parquet processing error: 'FAIL'")
    mock_dlq.send_message.assert_called()
    mock_queue.Message().delete.assert_called()


@patch("os.path.exists", MagicMock(return_value=True))
@patch("os.remove")
@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_and_write")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.s3", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
@patch("backend.ecs_tasks.delete_files.delete_files.dlq")
@patch("backend.ecs_tasks.delete_files.delete_files.check_file_size", MagicMock())
def test_it_handles_arrow_exceptions(mock_dlq, mock_queue, mock_log, mock_load_parquet, mock_delete_write, mock_remove):
    # Arrange
    mock_delete_write.side_effect = ArrowException("FAIL")
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load_parquet.return_value = parquet_file
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_remove.assert_called()
    mock_log.assert_called_with(ANY, "Parquet processing error: FAIL")
    mock_dlq.send_message.assert_called()
    mock_queue.Message().delete.assert_called()


@patch("os.path.exists", MagicMock(return_value=False))
@patch("os.remove", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
@patch("backend.ecs_tasks.delete_files.delete_files.dlq")
def test_it_validates_messages_with_missing_keys(mock_dlq, mock_queue, mock_log):
    # Act
    execute("{}", "receipt_handle")
    # Assert
    mock_log.assert_not_called()
    mock_dlq.send_message.assert_called()
    mock_queue.Message().delete.assert_called()


@patch("os.path.exists", MagicMock(return_value=False))
@patch("os.remove", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
@patch("backend.ecs_tasks.delete_files.delete_files.dlq")
def test_it_validates_messages_with_invalid_body(mock_dlq, mock_queue, mock_log):
    # Act
    execute("NOT JSON", "receipt_handle")
    # Assert
    mock_log.assert_not_called()
    mock_dlq.send_message.assert_called()
    mock_queue.Message().delete.assert_called()


@patch("os.path.exists", MagicMock(return_value=False))
@patch("os.remove", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
@patch("backend.ecs_tasks.delete_files.delete_files.dlq")
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.check_file_size", MagicMock())
def test_it_handles_s3_permission_issues(mock_s3, mock_dlq, mock_queue, mock_log):
    mock_s3.open.side_effect = ClientError({}, "GetObject")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    msg = mock_log.call_args[0][1]
    assert msg.startswith("Unable to retrieve object:")
    mock_dlq.send_message.assert_called()
    mock_queue.Message().delete.assert_called()


@patch("os.path.exists", MagicMock(return_value=False))
@patch("os.remove", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.check_file_size")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
@patch("backend.ecs_tasks.delete_files.delete_files.dlq")
@patch("backend.ecs_tasks.delete_files.delete_files.s3", MagicMock())
def test_it_handles_file_too_big(mock_dlq, mock_queue, mock_check_size, mock_log):
    # Arrange
    mock_check_size.side_effect = IOError("Too big")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_log.assert_called_with(ANY, "Unable to retrieve object: Too big")
    mock_dlq.send_message.assert_called()
    mock_queue.Message().delete.assert_called()


@patch("backend.ecs_tasks.delete_files.delete_files.get_max_file_size_bytes", MagicMock(return_value=9 * math.pow(
    1024, 3)))
def test_it_permits_files_under_max_size():
    mock_s3 = MagicMock()
    mock_s3.size.return_value = 8 * math.pow(1024, 3)
    check_file_size(mock_s3, "some_path")


@patch("backend.ecs_tasks.delete_files.delete_files.get_max_file_size_bytes", MagicMock(return_value=9 * math.pow(
    1024, 3)))
def test_it_throws_if_file_too_big():
    mock_s3 = MagicMock()
    mock_s3.size.return_value = 10 * math.pow(1024, 3)
    with pytest.raises(IOError):
        check_file_size(mock_s3, "some_path")


@patch.dict(os.environ, {"MAX_FILE_SIZE_GB": "5"})
def test_it_reads_max_size_from_env():
    resp = get_max_file_size_bytes()
    assert resp == 5 * math.pow(1024, 3)


def test_it_defaults_max_file_size():
    resp = get_max_file_size_bytes()
    assert resp == 9 * math.pow(1024, 3)


def message_stub(**kwargs):
    return json.dumps({
        "JobId": "1234",
        "Object": "s3://bucket/path/basic.parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": ["12345", "23456"]}],
        **kwargs
    })
