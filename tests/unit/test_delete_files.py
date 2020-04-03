import datetime
import os
from io import BytesIO

import boto3
from botocore.exceptions import ClientError
from mock import patch, MagicMock, mock_open, ANY, call

import json
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import pandas as pd
from pyarrow.lib import ArrowException

with patch.dict(os.environ, {
    "DELETE_OBJECTS_QUEUE": "https://url/q.fifo",
    "DLQ": "https://url/q",
}):
    from backend.ecs_tasks.delete_files.delete_files import execute, get_emitter_id, \
    emit_deletion_event, emit_failure_event, save, get_grantees, \
    get_object_info, get_object_tags, get_object_acl, get_requester_payment, get_row_count, \
    delete_from_dataframe, delete_matches_from_file, load_parquet, kill_handler, handle_error, \
    validate_bucket_versioning, sanitize_message, verify_object_versions_integrity, \
    retry_wrapper, IntegrityCheckFailedError, rollback_object_version, delete_old_versions, \
    DeleteOldVersionsError

pytestmark = [pytest.mark.unit]


def message_stub(**kwargs):
    return json.dumps({
        "JobId": "1234",
        "Object": "s3://bucket/path/basic.parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": ["12345", "23456"]}],
        "DeleteOldVersions": False,
        **kwargs
    })


def get_list_object_versions_error():
    return ClientError({
        'Error': {
            'Code': 'InvalidArgument',
            'Message': 'Invalid version id specified'
        }
    }, "ListObjectVersions")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.delete_files.get_session")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
def test_happy_path_when_queue_not_empty(mock_save, mock_emit, mock_delete, mock_s3, mock_load, mock_session, mock_verify_integrity):
    mock_s3.S3FileSystem.return_value = mock_s3
    column = {"Column": "customer_id", "MatchIds": ["12345", "23456"]}
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_save.return_value="new_version123"
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = MagicMock(version_id="abc123")
    mock_load.return_value = parquet_file
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(message_stub(Object="s3://bucket/path/basic.parquet"), "receipt_handle")
    mock_s3.open.assert_called_with("s3://bucket/path/basic.parquet", "rb")
    mock_delete.assert_called_with(parquet_file, [column])
    mock_save.assert_called_with(ANY, ANY, ANY, "bucket", "path/basic.parquet", "abc123")
    mock_emit.assert_called()
    mock_session.assert_called_with(None)
    mock_verify_integrity.assert_called_with(ANY, 'bucket', 'path/basic.parquet', 'abc123', 'new_version123')
    buf = mock_save.call_args[0][2]
    assert buf.read
    assert isinstance(buf, pa.BufferReader)  # must be BufferReader for zero-copy


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.verify_object_versions_integrity", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.save", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_session")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
def test_it_assumes_role(mock_delete, mock_s3, mock_load, mock_session):
    mock_s3.S3FileSystem.return_value = mock_s3
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = MagicMock(version_id="abc123")
    mock_load.return_value = parquet_file
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(message_stub(RoleArn="arn:aws:iam:account_id:role/rolename",
                         Object="s3://bucket/path/basic.parquet"), "receipt_handle")
    mock_session.assert_called_with("arn:aws:iam:account_id:role/rolename")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.verify_object_versions_integrity", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.save")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_old_versions")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
def test_it_removes_old_versions(mock_delete, mock_s3, mock_load, mock_delete_versions, mock_save):
    mock_s3.S3FileSystem.return_value = mock_s3
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = MagicMock(version_id="abc123")
    mock_save.return_value = "new_version123"
    mock_load.return_value = parquet_file
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(message_stub(RoleArn="arn:aws:iam:account_id:role/rolename",
                         DeleteOldVersions=True,
                         Object="s3://bucket/path/basic.parquet"), "receipt_handle")
    mock_delete_versions.assert_called_with(ANY, ANY, ANY, "new_version123")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.verify_object_versions_integrity", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.save")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_old_versions")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_old_version_delete_failures(mock_handle, mock_delete, mock_s3, mock_load, mock_delete_versions,
                                                mock_save):
    mock_s3.S3FileSystem.return_value = mock_s3
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = MagicMock(version_id="abc123")
    mock_save.return_value = "new_version123"
    mock_load.return_value = parquet_file
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    mock_delete_versions.side_effect = DeleteOldVersionsError(errors=["access denied"])
    execute(message_stub(RoleArn="arn:aws:iam:account_id:role/rolename",
                         DeleteOldVersions=True,
                         Object="s3://bucket/path/basic.parquet"), "receipt_handle")
    mock_handle.assert_called_with(ANY, ANY, "Unable to delete previous versions: access denied")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
@patch("backend.ecs_tasks.delete_files.delete_files.logger")
def test_warning_logged_for_no_deletions(mock_logger, mock_save, mock_emit, mock_delete, mock_s3, mock_load):
    mock_s3.S3FileSystem.return_value = mock_s3
    column = {"Column": "customer_id",
              "MatchIds": ["12345", "23456"]}
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 0}
    execute(message_stub(Object="s3://bucket/path/basic.parquet"), "receipt_handle")
    mock_s3.open.assert_called_with("s3://bucket/path/basic.parquet", "rb")
    mock_delete.assert_called_with(parquet_file, [column])
    mock_save.assert_not_called()
    mock_logger.warning.assert_called()
    mock_emit.assert_called()


@patch("backend.ecs_tasks.delete_files.delete_files.delete_from_dataframe")
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


@patch("backend.ecs_tasks.delete_files.delete_files.emit_event")
@patch("backend.ecs_tasks.delete_files.delete_files.get_emitter_id")
def test_it_emits_deletions(mock_get_id, mock_emit):
    mock_get_id.return_value = "ECSTask_4567"
    stats_stub = {"Some": "stats"}
    msg = json.loads(message_stub())
    emit_deletion_event(msg, stats_stub)
    mock_emit.assert_called_with("1234", "ObjectUpdated", {
        "Statistics": stats_stub,
        "Object": "s3://bucket/path/basic.parquet",
    }, 'ECSTask_4567')


@patch("backend.ecs_tasks.delete_files.delete_files.emit_event")
@patch("backend.ecs_tasks.delete_files.delete_files.get_emitter_id")
def test_it_emits_failed_deletions(mock_get_id, mock_emit):
    mock_get_id.return_value = "ECSTask_4567"
    msg = message_stub()
    emit_failure_event(msg, "Some error", "ObjectUpdateFailed")
    mock_emit.assert_called_with("1234", "ObjectUpdateFailed", {
        "Error": "Some error",
        "Message": json.loads(msg)
    }, 'ECSTask_4567')


@patch("backend.ecs_tasks.delete_files.delete_files.emit_event")
@patch("backend.ecs_tasks.delete_files.delete_files.get_emitter_id")
def test_it_emits_failed_rollback(mock_get_id, mock_emit):
    mock_get_id.return_value = "ECSTask_4567"
    msg = message_stub()
    emit_failure_event(msg, "Some error", "ObjectRollbackFailed")
    mock_emit.assert_called_with("1234", "ObjectRollbackFailed", {
        "Error": "Some error",
        "Message": json.loads(msg)
    }, 'ECSTask_4567')


def test_it_raises_for_missing_job_id():
    with pytest.raises(ValueError):
        emit_failure_event("{}", "Some error", "deletion")


@patch("backend.ecs_tasks.delete_files.delete_files.sanitize_message")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failure_event")
def test_it_gracefully_handles_invalid_message_bodies(mock_emit, mock_sanitize):
    sqs_message = MagicMock()
    mock_emit.side_effect = ValueError("Bad message")
    handle_error(sqs_message, "{}", "Some error")
    # Verify it attempts to emit the failure
    mock_sanitize.assert_called()
    mock_emit.assert_called()
    # Verify even if emitting fails, the message visibility changes
    sqs_message.change_visibility.assert_called()


@patch("backend.ecs_tasks.delete_files.delete_files.sanitize_message")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failure_event")
def test_it_gracefully_handles_invalid_job_id(mock_emit, mock_sanitize):
    sqs_message = MagicMock()
    mock_emit.side_effect = KeyError("Invalid Job ID")
    handle_error(sqs_message, "{}", "Some error")
    # Verify it attempts to emit the failure
    mock_sanitize.assert_called()
    mock_emit.assert_called()
    # Verify even if emitting fails, the message visibility changes
    sqs_message.change_visibility.assert_called()


@patch("backend.ecs_tasks.delete_files.delete_files.sanitize_message")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failure_event")
def test_it_gracefully_handles_client_errors(mock_emit, mock_sanitize):
    sqs_message = MagicMock()
    mock_emit.side_effect = ClientError({}, "PutItem")
    handle_error(sqs_message, "{}", "Some error")
    # Verify it attempts to emit the failure
    mock_sanitize.assert_called()
    mock_emit.assert_called()
    # Verify even if emitting fails, the message visibility changes
    sqs_message.change_visibility.assert_called()


@patch("backend.ecs_tasks.delete_files.delete_files.sanitize_message")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failure_event")
def test_it_doesnt_change_message_visibility_when_rollback_fails(mock_emit, mock_sanitize):
    sqs_message = MagicMock()
    mock_emit.side_effect = ClientError({}, "DeleteObjectVersion")
    handle_error(sqs_message, "{}", "Some error", "ObjectRollbackFailed", False)
    # Verify it attempts to emit the failure
    mock_sanitize.assert_called()
    mock_emit.assert_called()
    # Verify that the visibility doesn't change for a rollback event
    sqs_message.change_visibility.assert_not_called()


@patch("backend.ecs_tasks.delete_files.delete_files.emit_failure_event")
def test_it_gracefully_handles_change_message_visibility_failure(mock_emit):
    sqs_message = MagicMock()
    e = boto3.client("sqs").exceptions.ReceiptHandleIsInvalid
    sqs_message.change_visibility.side_effect = e({}, "ReceiptHandleIsInvalid")
    handle_error(sqs_message, "{}", "Some error")
    # Verify it attempts to emit the failure
    mock_emit.assert_called()
    sqs_message.change_visibility.assert_called()  # Implicit graceful handling


@patch("os.getenv", MagicMock(return_value="/some/path"))
@patch("os.path.isfile", MagicMock(return_value=True))
def test_it_loads_task_id_from_metadata():
    get_emitter_id.cache_clear()
    with patch("builtins.open", mock_open(read_data="{\"TaskARN\": \"arn:aws:ecs:us-west-2:012345678910:task/default/2b88376d-aba3-4950-9ddf-bcb0f388a40c\"}")):
        resp = get_emitter_id()
        assert "ECSTask_2b88376d-aba3-4950-9ddf-bcb0f388a40c" == resp


@patch("os.getenv", MagicMock(return_value=None))
def test_it_provides_default_id():
    get_emitter_id.cache_clear()
    resp = get_emitter_id()
    assert "ECSTask" == resp


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_missing_col_exceptions(mock_error_handler, mock_delete, mock_load):
    # Arrange
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_delete.side_effect = KeyError("FAIL")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(ANY, ANY, "Parquet processing error: 'FAIL'")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_arrow_exceptions(mock_error_handler, mock_delete, mock_load):
    # Arrange
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_delete.side_effect = ArrowException("FAIL")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(ANY, ANY, "Parquet processing error: FAIL")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_validates_messages_with_missing_keys(mock_error_handler):
    # Act
    execute("{}", "receipt_handle")
    # Assert
    mock_error_handler.assert_called()


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_validates_messages_with_invalid_body(mock_error_handler):
    # Act
    execute("NOT JSON", "receipt_handle")
    mock_error_handler.assert_called()


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_s3_permission_issues(mock_error_handler, mock_s3):
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.side_effect = ClientError({}, "GetObject")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    msg = mock_error_handler.call_args[0][2]
    assert msg.startswith("ClientError:")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_io_errors(mock_error_handler, mock_s3):
    # Arrange
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.side_effect = IOError("an error")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(ANY, ANY, "Unable to retrieve object: an error")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_file_too_big(mock_error_handler, mock_s3):
    # Arrange
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.side_effect = MemoryError("Too big")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(ANY, ANY, "Insufficient memory to work on object: Too big")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_generic_error(mock_error_handler, mock_s3):
    # Arrange
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.side_effect = RuntimeError("Some Error")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(ANY, ANY, "Unknown error during message processing: Some Error")


@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning")
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_unversioned_buckets(mock_error_handler, mock_s3, mock_versioning):
    # Arrange
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_versioning.side_effect = ValueError("Versioning validation Error")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(ANY, ANY, "Unprocessable message: Versioning validation Error")
    mock_versioning.assert_called_with(ANY, 'bucket')


def test_it_validates_bucket_versioning():
    validate_bucket_versioning.cache_clear()
    client = MagicMock()
    client.get_bucket_versioning.return_value = {"Status": "Enabled"}
    assert validate_bucket_versioning(client, "bucket")


def test_it_throws_when_versioning_disabled():
    validate_bucket_versioning.cache_clear()
    client = MagicMock()
    client.get_bucket_versioning.return_value = {}

    with pytest.raises(ValueError) as e:
        validate_bucket_versioning(client, "bucket")

    assert e.value.args[0] == 'Bucket bucket does not have versioning enabled'


def test_it_throws_when_versioning_suspended():
    validate_bucket_versioning.cache_clear()
    client = MagicMock()
    client.get_bucket_versioning.return_value = {"Status": "Suspended"}

    with pytest.raises(ValueError) as e:
        validate_bucket_versioning(client, "bucket")

    assert e.value.args[0] == 'Bucket bucket does not have versioning enabled'


def test_it_throws_when_mfa_delete_enabled():
    validate_bucket_versioning.cache_clear()
    client = MagicMock()
    client.get_bucket_versioning.return_value = {"Status": "Enabled", "MFADelete": "Enabled"}

    with pytest.raises(ValueError) as e:
        validate_bucket_versioning(client, "bucket")

    assert e.value.args[0] == 'Bucket bucket has MFA Delete enabled'


def test_it_returns_requester_pays():
    get_requester_payment.cache_clear()
    client = MagicMock()
    client.get_bucket_request_payment.return_value = {"Payer": "Requester"}
    assert ({"RequestPayer": "requester"}, {"Payer": "Requester"}) == get_requester_payment(client, "bucket")


def test_it_returns_empty_for_non_requester_pays():
    get_requester_payment.cache_clear()
    client = MagicMock()
    client.get_bucket_request_payment.return_value = {"Payer": "Owner"}
    assert ({}, {"Payer": "Owner"}) == get_requester_payment(client, "bucket")


@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
def test_it_returns_standard_info(mock_requester):
    get_object_info.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    stub = {
        'CacheControl': "cache",
        'ContentDisposition': "content_disposition",
        'ContentEncoding': "content_encoding",
        'ContentLanguage': "content_language",
        'ContentType': "ContentType",
        'Expires': "123",
        'Metadata': {"foo": "bar"},
        'ServerSideEncryption': "see",
        'StorageClass': "STANDARD",
        'SSECustomerAlgorithm': "aws:kms",
        'SSEKMSKeyId': "1234",
        'WebsiteRedirectLocation': "test"
    }
    client.head_object.return_value = stub
    assert stub == get_object_info(client, "bucket", "key")[0]


@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
def test_it_strips_empty_standard_info(mock_requester):
    get_object_info.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    stub = {
        'CacheControl': "cache",
        'ContentDisposition': "content_disposition",
        'ContentEncoding': "content_encoding",
        'ContentLanguage': "content_language",
        'ContentType': "ContentType",
        'Expires': "123",
        'Metadata': {"foo": "bar"},
        'ServerSideEncryption': "see",
        'StorageClass': "STANDARD",
        'SSECustomerAlgorithm': "aws:kms",
        'SSEKMSKeyId': "1234",
        'WebsiteRedirectLocation': None
    }
    client.head_object.return_value = stub
    assert {
        'CacheControl': "cache",
        'ContentDisposition': "content_disposition",
        'ContentEncoding': "content_encoding",
        'ContentLanguage': "content_language",
        'ContentType': "ContentType",
        'Expires': "123",
        'Metadata': {"foo": "bar"},
        'ServerSideEncryption': "see",
        'StorageClass': "STANDARD",
        'SSECustomerAlgorithm': "aws:kms",
        'SSEKMSKeyId': "1234",
    } == get_object_info(client, "bucket", "key")[0]


@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
def test_it_handles_versions_for_get_info(mock_requester):
    get_object_info.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    client.head_object.return_value = {}
    get_object_info(client, "bucket", "key")
    client.head_object.assert_called_with(Bucket="bucket", Key="key")
    get_object_info(client, "bucket", "key", "abc123")
    client.head_object.assert_called_with(Bucket="bucket", Key="key", VersionId="abc123")


@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
def test_it_gets_tagging_args(mock_requester):
    get_object_tags.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    client.get_object_tagging.return_value = {
        "TagSet": [{"Key": "a", "Value": "b"}, {"Key": "c", "Value": "d"}]
    }
    assert {
        'Tagging': "a=b&c=d",
    } == get_object_tags(client, "bucket", "key")[0]


@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
def test_it_handles_versions_for_get_tagging(mock_requester):
    get_object_info.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    client.get_object_tagging.return_value = {"TagSet": []}
    get_object_tags(client, "bucket", "key")
    client.get_object_tagging.assert_called_with(Bucket="bucket", Key="key")
    get_object_tags(client, "bucket", "key", "abc123")
    client.get_object_tagging.assert_called_with(Bucket="bucket", Key="key", VersionId="abc123")


@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
def test_it_gets_acl_args(mock_requester):
    get_object_acl.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    client.get_object_acl.return_value = {
        "Owner": {"ID": "a"},
        "Grants": [{
            "Grantee": {'ID': 'b', 'Type': 'CanonicalUser'},
            "Permission": "READ"
        }, {
            "Grantee": {'ID': 'c', 'Type': 'CanonicalUser'},
            "Permission": "READ_ACP"
        }]
    }
    assert {
        'GrantFullControl': "id=a",
        'GrantRead': "id=b",
        'GrantReadACP': "id=c",
    } == get_object_acl(client, "bucket", "key")[0]


@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
def test_it_handles_versions_for_get_acl(mock_requester):
    get_object_info.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    client.get_object_tagging.return_value = {
        "Owner": {"ID": "a"},
        "Grants": [{
            "Grantee": {'ID': 'b', 'Type': 'CanonicalUser'},
            "Permission": "READ"
        }, {
            "Grantee": {'ID': 'c', 'Type': 'CanonicalUser'},
            "Permission": "READ_ACP"
        }]
    }
    get_object_acl(client, "bucket", "key")
    client.get_object_acl.assert_called_with(Bucket="bucket", Key="key")
    get_object_acl(client, "bucket", "key", "abc123")
    client.get_object_acl.assert_called_with(Bucket="bucket", Key="key", VersionId="abc123")


def test_it_gets_grantees_by_type():
    acl = {
        'Owner': {'ID': 'owner_id'},
        'Grants': [
            {'Grantee': {'ID': 'grantee1', 'Type': 'CanonicalUser'}, 'Permission': 'FULL_CONTROL'},
            {'Grantee': {'ID': 'grantee2', 'Type': 'CanonicalUser'}, 'Permission': 'FULL_CONTROL'},
            {'Grantee': {'EmailAddress': 'grantee3', 'Type': 'AmazonCustomerByEmail'}, 'Permission': 'READ'},
            {'Grantee': {'URI': 'grantee4', 'Type': 'Group'}, 'Permission': 'WRITE'},
            {'Grantee': {'ID': 'grantee5', 'Type': 'CanonicalUser'}, 'Permission': 'READ_ACP'},
            {'Grantee': {'ID': 'grantee6', 'Type': 'CanonicalUser'}, 'Permission': 'WRITE_ACP'},
        ]
    }
    assert {"id=grantee1", "id=grantee2"} == get_grantees(acl, "FULL_CONTROL")
    assert {"emailAddress=grantee3"} == get_grantees(acl, "READ")
    assert {"uri=grantee4"} == get_grantees(acl, "WRITE")
    assert {"id=grantee5"} == get_grantees(acl, "READ_ACP")
    assert {"id=grantee6"} == get_grantees(acl, "WRITE_ACP")


@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_info")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_tags")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_acl")
@patch("backend.ecs_tasks.delete_files.delete_files.get_grantees")
def test_it_applies_settings_when_saving(mock_grantees, mock_acl, mock_tagging, mock_standard, mock_requester, mock_s3):
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_client = MagicMock()
    mock_requester.return_value = {"RequestPayer": "requester"}, {"Payer": "Requester"}
    mock_standard.return_value = ({
        "Expires": "123",
        "Metadata": {}
    }, {})
    mock_tagging.return_value = ({"Tagging": "a=b"}, {"TagSet": [{"Key": "a", "Value": "b"}]})
    mock_acl.return_value = ({
        "GrantFullControl": "id=abc",
        "GrantRead": "id=123",
    }, {
        'Owner': {'ID': 'owner_id'},
        'Grants': [
            {'Grantee': {'ID': 'abc', 'Type': 'CanonicalUser'}, 'Permission': 'FULL_CONTROL'},
            {'Grantee': {'ID': '123', 'Type': 'CanonicalUser'}, 'Permission': 'READ'},
        ]
    })
    mock_grantees.return_value = ''
    buf = BytesIO()
    mock_file = MagicMock(version_id="abc123")
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = mock_file
    resp = save(mock_s3, mock_client, buf, "bucket", "key", "abc123")
    mock_file.write.assert_called_with(b'')
    assert "abc123" == resp
    mock_client.put_object_acl.assert_not_called()


@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_info")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_tags")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_acl")
@patch("backend.ecs_tasks.delete_files.delete_files.get_grantees")
def test_it_passes_through_version(mock_grantees, mock_acl, mock_tagging, mock_standard, mock_requester, mock_s3):
    mock_client = MagicMock()
    mock_requester.return_value = {}, {}
    mock_standard.return_value = ({}, {})
    mock_tagging.return_value = ({}, {})
    mock_acl.return_value = ({}, {})
    mock_grantees.return_value = ''
    buf = BytesIO()
    save(mock_s3, mock_client, buf, "bucket", "key", "abc123")
    mock_acl.assert_called_with(mock_client, "bucket", "key", "abc123")
    mock_tagging.assert_called_with(mock_client, "bucket", "key", "abc123")
    mock_standard.assert_called_with(mock_client, "bucket", "key", "abc123")


@patch("backend.ecs_tasks.delete_files.delete_files.s3fs")
@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_info")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_tags")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_acl")
@patch("backend.ecs_tasks.delete_files.delete_files.get_grantees")
def test_it_restores_write_permissions(mock_grantees, mock_acl, mock_tagging, mock_standard, mock_requester, mock_s3):
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_client = MagicMock()
    mock_requester.return_value = {}, {}
    mock_standard.return_value = ({}, {})
    mock_tagging.return_value = ({}, {})
    mock_acl.return_value = ({
        "GrantFullControl": "id=abc",
    }, {
        'Owner': {'ID': 'owner_id'},
        'Grants': [
            {'Grantee': {'ID': 'abc', 'Type': 'CanonicalUser'}, 'Permission': 'FULL_CONTROL'},
            {'Grantee': {'ID': '123', 'Type': 'CanonicalUser'}, 'Permission': 'WRITE'},
        ]
    })
    mock_grantees.return_value = {"id=123"}
    buf = BytesIO()
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = MagicMock(version_id="new_version123")
    save(mock_s3, mock_client, buf, "bucket", "key", "abc123")
    mock_client.put_object_acl.assert_called_with(
        Bucket="bucket",
        Key="key",
        VersionId="new_version123",
        GrantFullControl="id=abc",
        GrantWrite="id=123"
    )


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
def test_it_provides_logs_for_acl_fail(mock_save, mock_error_handler, mock_delete, mock_load):
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_save.side_effect = ClientError({}, "PutObjectAcl")
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(message_stub(), "receipt_handle")
    mock_save.assert_called()
    mock_error_handler.assert_called_with(ANY, ANY, "ClientError: An error occurred (Unknown) when calling the PutObjectAcl "
                                                    "operation: Unknown. Redacted object uploaded successfully but unable to "
                                                    "restore WRITE ACL")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.save", MagicMock(return_value="new_version"))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.rollback_object_version")
@patch("backend.ecs_tasks.delete_files.delete_files.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_provides_logs_for_failed_version_integrity_check_and_performs_rollback(mock_error_handler, mock_delete, mock_load, mock_verify_integrity, rollback_mock):
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_verify_integrity.side_effect = IntegrityCheckFailedError(
        "Some error",
        MagicMock(),
        'bucket',
        'path/basic.parquet',
        'new_version')

    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(message_stub(), "receipt_handle")
    mock_verify_integrity.assert_called()
    mock_error_handler.assert_called_with(ANY, ANY, "Object version integrity check failed: Some error")
    rollback_mock.assert_called_with(ANY, 'bucket', 'path/basic.parquet', 'new_version')


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.save", MagicMock(return_value="new_version"))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_provides_logs_for_get_latest_version_fail(mock_error_handler, mock_delete, mock_load, mock_verify_integrity):
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_verify_integrity.side_effect = get_list_object_versions_error()
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(message_stub(), "receipt_handle")
    mock_verify_integrity.assert_called()
    mock_error_handler.assert_called_with(ANY, ANY, "ClientError: An error occurred (InvalidArgument) when calling the "
                                                    "ListObjectVersions operation: Invalid version id specified. Could "
                                                    "not verify redacted object version integrity")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.save", MagicMock(return_value="new_version"))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_provides_logs_for_failed_rollback_client_error(mock_error_handler, mock_delete, mock_load, mock_verify_integrity):
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_s3 = MagicMock()
    mock_s3.delete_object.side_effect = ClientError({}, "DeleteObject")
    mock_verify_integrity.side_effect = IntegrityCheckFailedError(
        "Some error",
        mock_s3,
        'bucket',
        'test/basic.parquet',
        'new_version')
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(message_stub(), "receipt_handle")
    mock_verify_integrity.assert_called()
    assert mock_error_handler.call_args_list == [
        call(ANY, ANY, "Object version integrity check failed: Some error"),
        call(ANY, ANY, "ClientError: An error occurred (Unknown) when calling the DeleteObject operation: Unknown. "
                       "Version rollback caused by version integrity conflict failed", "ObjectRollbackFailed", False)]


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.save", MagicMock(return_value="new_version"))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_provides_logs_for_failed_rollback_generic_error(mock_error_handler, mock_delete, mock_load, mock_verify_integrity):
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_s3 = MagicMock()
    mock_s3.delete_object.side_effect = Exception("error!!")
    mock_verify_integrity.side_effect = IntegrityCheckFailedError(
        "Some error",
        mock_s3,
        'bucket',
        'test/basic.parquet',
        'new_version')
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(message_stub(), "receipt_handle")
    mock_verify_integrity.assert_called()
    assert mock_error_handler.call_args_list == [
        call(ANY, ANY, "Object version integrity check failed: Some error"),
        call(ANY, ANY, "Unknown error: error!!. Version rollback caused by version integrity conflict failed",
                       "ObjectRollbackFailed", False)]


def test_it_loads_parquet_files():
    data = [{'customer_id': '12345'}, {'customer_id': '23456'}]
    df = pd.DataFrame(data)
    buf = BytesIO()
    df.to_parquet(buf, compression="snappy")
    resp = load_parquet(buf)
    assert 2 == resp.read().num_rows


@patch("backend.ecs_tasks.delete_files.delete_files.emit_failure_event")
def test_error_handler(mock_emit):
    msg = MagicMock()
    handle_error(msg, "{}", "Test Error")
    mock_emit.assert_called_with("{}", "Test Error", "ObjectUpdateFailed")
    msg.change_visibility.assert_called_with(VisibilityTimeout=0)


@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_kill_handler_cleans_up(mock_error_handler):
    with pytest.raises(SystemExit) as e:
        mock_pool = MagicMock()
        mock_msg = MagicMock()
        kill_handler([mock_msg], mock_pool)
        mock_pool.terminate.assert_called()
        mock_error_handler.assert_called()
        assert 1 == e.value.code


@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_kill_handler_exits_successfully_when_done(mock_error_handler):
    with pytest.raises(SystemExit) as e:
        mock_pool = MagicMock()
        kill_handler([], mock_pool)
        mock_pool.terminate.assert_called()
        mock_error_handler.assert_not_called()
        assert 0 == e.value.code


@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_gracefully_handles_cleanup_issues(mock_error_handler):
    with pytest.raises(SystemExit):
        mock_pool = MagicMock()
        mock_msg = MagicMock()
        mock_error_handler.side_effect = ValueError()
        kill_handler([mock_msg, mock_msg], mock_pool)
        assert 2 == mock_error_handler.call_count
        mock_pool.terminate.assert_called()


def test_it_sanitises_matches():
    assert "This message contains ID *** MATCH ID *** and *** MATCH ID ***" == sanitize_message(
        "This message contains ID 12345 and 23456", message_stub(Columns=[{
            "Column": "a", "MatchIds": ["12345", "23456", "34567"]
        }]))


def test_sanitiser_handles_malformed_messages():
    assert "an error message" == sanitize_message("an error message", "not json")


def test_it_verifies_integrity_happy_path():
    s3_mock = MagicMock()
    s3_mock.list_object_versions.return_value = {
        "VersionIdMarker": "v7",
        "Versions": [{ "VersionId": "v6", "ETag": "a" }]
    }
    result = verify_object_versions_integrity(s3_mock, 'bucket', 'requirements.txt', 'v6', 'v7')

    assert result
    s3_mock.list_object_versions.assert_called_with(
        Bucket='bucket',
        Prefix='requirements.txt',
        VersionIdMarker='v7',
        KeyMarker='requirements.txt',
        MaxKeys=1)


def test_it_fails_integrity_when_delete_marker_between():
    s3_mock = MagicMock()
    s3_mock.list_object_versions.return_value = {
        "VersionIdMarker": "v7",
        "Versions": [],
        "DeleteMarkers": [{ "VersionId": "v6" }]
    }

    with pytest.raises(IntegrityCheckFailedError) as e:
        result = verify_object_versions_integrity(s3_mock, 'bucket', 'requirements.txt', 'v5', 'v7')
    assert e.value.args == (
        'A delete marker (v6) was detected for the given object between read and write operations (v5 and v7).',
        s3_mock,
        'bucket',
        'requirements.txt',
        'v7')


def test_it_fails_integrity_when_other_version_between():
    s3_mock = MagicMock()
    s3_mock.list_object_versions.return_value = {
        "VersionIdMarker": "v7",
        "Versions": [{ "VersionId": "v6", "ETag": "a" }]
    }

    with pytest.raises(IntegrityCheckFailedError) as e:
        result = verify_object_versions_integrity(s3_mock, 'bucket', 'requirements.txt', 'v5', 'v7')

    assert e.value.args == (
        'A version (v6) was detected for the given object between read and write operations (v5 and v7).',
        s3_mock,
        'bucket',
        'requirements.txt',
        'v7')


def test_it_fails_integrity_when_no_other_version_before():
    s3_mock = MagicMock()
    s3_mock.list_object_versions.return_value = {
        "VersionIdMarker": "v7",
        "Versions": []
    }

    with pytest.raises(IntegrityCheckFailedError) as e:
        result = verify_object_versions_integrity(s3_mock, 'bucket', 'requirements.txt', 'v5', 'v7')

    assert e.value.args == (
        'Previous version (v5) has been deleted.',
        s3_mock,
        'bucket',
        'requirements.txt',
        'v7')

@patch("time.sleep")
def test_it_errors_when_version_to_not_found_after_retries(sleep_mock):
    s3_mock = MagicMock()
    s3_mock.list_object_versions.side_effect = get_list_object_versions_error()

    with pytest.raises(ClientError) as e:
        result = verify_object_versions_integrity(s3_mock, 'bucket', 'requirements.txt', 'v7', 'v8')

    assert sleep_mock.call_args_list == [call(2), call(4), call(8), call(16), call(32)]
    assert e.value.args[0] == 'An error occurred (InvalidArgument) when calling the ListObjectVersions operation: Invalid version id specified'


@patch("time.sleep")
def test_it_doesnt_retry_success_fn(sleep_mock):
    fn = MagicMock()
    fn.side_effect = [31, 32]
    result = retry_wrapper(fn, retry_wait_seconds=1, retry_factor=3)(25)

    assert result == 31
    assert fn.call_args_list == [call(25)]
    assert not sleep_mock.called


@patch("time.sleep")
def test_it_retries_retriable_fn(sleep_mock):
    fn = MagicMock()
    e = get_list_object_versions_error()
    fn.side_effect = [e, e, 32]
    result = retry_wrapper(fn, retry_wait_seconds=1, retry_factor=3)(22)

    assert result == 32
    assert fn.call_args_list == [call(22), call(22), call(22)]
    assert sleep_mock.call_args_list == [call(1), call(3)]


@patch("time.sleep")
def test_it_doesnt_retry_non_retriable_fn(sleep_mock):
    fn = MagicMock()
    fn.side_effect = NameError("fail!")

    with pytest.raises(NameError) as e:
        result = retry_wrapper(fn, retry_wait_seconds=1, retry_factor=3)(22)

    assert e.value.args[0] == 'fail!'
    assert fn.call_args_list == [call(22)]
    assert not sleep_mock.called


@patch("time.sleep")
def test_it_retries_and_gives_up_fn(sleep_mock):
    fn = MagicMock()
    fn.side_effect = get_list_object_versions_error()

    with pytest.raises(ClientError) as e:
        result = retry_wrapper(fn, max_retries=3)(22)

    assert e.value.args[0] == 'An error occurred (InvalidArgument) when calling the ListObjectVersions operation: Invalid version id specified'
    assert fn.call_args_list == [call(22), call(22), call(22), call(22)]
    assert sleep_mock.call_args_list == [call(2), call(4), call(8)]


def test_it_deletes_new_version_during_rollback():
    s3_mock = MagicMock()
    s3_mock.delete_object.return_value = "result"
    result = rollback_object_version(s3_mock, 'bucket', 'requirements.txt', "version23")
    assert result == "result"
    s3_mock.delete_object.assert_called_with(
        Bucket='bucket',
        Key='requirements.txt',
        VersionId='version23')


@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_error_for_client_error(mock_error_handler):
    s3_mock = MagicMock()
    s3_mock.delete_object.side_effect = ClientError({}, "DeleteObject")
    result = rollback_object_version(s3_mock, 'bucket', 'requirements.txt', "version23")
    mock_error_handler.assert_called_with(
        ANY,
        ANY,
        "ClientError: An error occurred (Unknown) when calling the DeleteObject operation: Unknown. "
        "Version rollback caused by version integrity conflict failed",
        "ObjectRollbackFailed",
        False)


@patch("backend.ecs_tasks.delete_files.delete_files.paginate")
def test_it_deletes_old_versions(paginate_mock):
    s3_mock = MagicMock()
    paginate_mock.return_value = iter([
        (
            {"VersionId": "v1", "LastModified": datetime.datetime.now() - datetime.timedelta(minutes=4)},
            {"VersionId": "d2", "LastModified": datetime.datetime.now() - datetime.timedelta(minutes=3)}
        ),
        (
            {"VersionId": "v3", "LastModified": datetime.datetime.now() - datetime.timedelta(minutes=2)},
            None
        )
    ])

    delete_old_versions(s3_mock, "bucket", "key", "v4")
    paginate_mock.assert_called_with(
        s3_mock,
        s3_mock.list_object_versions,
        ["Versions", "DeleteMarkers"],
        Bucket="bucket",
        Prefix='key',
        VersionIdMarker='v4',
        KeyMarker='key'
    )
    s3_mock.delete_objects.assert_called_with(
        Bucket="bucket",
        Delete={
            'Objects': [
                {'Key': "key", 'VersionId': "v1"},
                {'Key': "key", 'VersionId': "d2"},
                {'Key': "key", 'VersionId': "v3"},
            ],
            'Quiet': True
        }
    )


@patch("backend.ecs_tasks.delete_files.delete_files.paginate")
def test_it_handles_high_old_version_count(paginate_mock):
    s3_mock = MagicMock()
    paginate_mock.return_value = iter([
        (
            {"VersionId": "v{}".format(i), "LastModified": datetime.datetime.now() + datetime.timedelta(minutes=i)},
            None
        ) for i in range(1, 1501)
    ])

    delete_old_versions(s3_mock, "bucket", "key", "v0")
    paginate_mock.assert_called_with(
        s3_mock,
        s3_mock.list_object_versions,
        ["Versions", "DeleteMarkers"],
        Bucket="bucket",
        Prefix='key',
        VersionIdMarker='v0',
        KeyMarker='key'
    )
    assert 2 == s3_mock.delete_objects.call_count
    assert {
        "Bucket": "bucket",
        "Delete": {
            'Objects': [
                {'Key': "key", 'VersionId': "v{}".format(i)} for i in range(1, 1001)
            ],
            'Quiet': True
        }
    } == s3_mock.delete_objects.call_args_list[0][1]
    assert {
        "Bucket": "bucket",
        "Delete": {
            'Objects': [
                {'Key': "key", 'VersionId': "v{}".format(i)} for i in range(1001, 1501)
            ],
            'Quiet': True
        }
    } == s3_mock.delete_objects.call_args_list[1][1]


@patch("backend.ecs_tasks.delete_files.delete_files.paginate")
def test_it_raises_for_deletion_errors(paginate_mock):
    s3_mock = MagicMock()
    paginate_mock.return_value = iter([
        (
            {"VersionId": "v1", "LastModified": datetime.datetime.now() - datetime.timedelta(minutes=4)},
            {"VersionId": "v2", "LastModified": datetime.datetime.now() - datetime.timedelta(minutes=3)}
        ),
        (
            {"VersionId": "v3", "LastModified": datetime.datetime.now() - datetime.timedelta(minutes=2)},
            None
        )
    ])
    s3_mock.delete_objects.return_value = {
        "Errors": [
            {"VersionId": "v1", "Key": "key", "Message": "Version not found"}
        ]
    }
    with pytest.raises(DeleteOldVersionsError):
        delete_old_versions(s3_mock, "bucket", "key", "v4")


@patch("backend.ecs_tasks.delete_files.delete_files.paginate")
def test_it_handles_client_errors_as_deletion_errors(paginate_mock):
    s3_mock = MagicMock()
    paginate_mock.side_effect = get_list_object_versions_error()
    with pytest.raises(DeleteOldVersionsError):
        delete_old_versions(s3_mock, "bucket", "key", "v3")
