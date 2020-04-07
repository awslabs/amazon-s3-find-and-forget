import os

import boto3
from botocore.exceptions import ClientError
from mock import patch, MagicMock, ANY, call

import json
import pyarrow as pa
import pytest
from pyarrow.lib import ArrowException

from s3 import DeleteOldVersionsError, IntegrityCheckFailedError

with patch.dict(os.environ, {
    "DELETE_OBJECTS_QUEUE": "https://url/q.fifo",
    "DLQ": "https://url/q",
}):
    from backend.ecs_tasks.delete_files.main import kill_handler, execute, handle_error

pytestmark = [pytest.mark.unit, pytest.mark.ecs_tasks]


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
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.get_session")
@patch("backend.ecs_tasks.delete_files.main.load_parquet")
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.main.save")
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
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.save", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session")
@patch("backend.ecs_tasks.delete_files.main.load_parquet")
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
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
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.save")
@patch("backend.ecs_tasks.delete_files.main.delete_old_versions")
@patch("backend.ecs_tasks.delete_files.main.load_parquet")
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
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
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.save")
@patch("backend.ecs_tasks.delete_files.main.delete_old_versions")
@patch("backend.ecs_tasks.delete_files.main.load_parquet")
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
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
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.load_parquet")
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.main.save")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_no_deletions(mock_handle, mock_save, mock_emit, mock_delete, mock_s3, mock_load):
    mock_s3.S3FileSystem.return_value = mock_s3
    column = {"Column": "customer_id", "MatchIds": ["12345", "23456"]}
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 0}
    execute(message_stub(Object="s3://bucket/path/basic.parquet"), "receipt_handle")
    mock_s3.open.assert_called_with("s3://bucket/path/basic.parquet", "rb")
    mock_delete.assert_called_with(parquet_file, [column])
    mock_save.assert_not_called()
    mock_emit.assert_not_called()
    mock_handle.assert_called_with(ANY, ANY, "Unprocessable message: The object s3://bucket/path/basic.parquet "
                                             "was processed successfully but no rows required deletion")


@patch("backend.ecs_tasks.delete_files.main.sanitize_message")
@patch("backend.ecs_tasks.delete_files.main.emit_failure_event")
def test_it_gracefully_handles_invalid_message_bodies(mock_emit, mock_sanitize):
    sqs_message = MagicMock()
    mock_emit.side_effect = ValueError("Bad message")
    handle_error(sqs_message, "{}", "Some error")
    # Verify it attempts to emit the failure
    mock_sanitize.assert_called()
    mock_emit.assert_called()
    # Verify even if emitting fails, the message visibility changes
    sqs_message.change_visibility.assert_called()


@patch("backend.ecs_tasks.delete_files.main.sanitize_message")
@patch("backend.ecs_tasks.delete_files.main.emit_failure_event")
def test_it_gracefully_handles_invalid_job_id(mock_emit, mock_sanitize):
    sqs_message = MagicMock()
    mock_emit.side_effect = KeyError("Invalid Job ID")
    handle_error(sqs_message, "{}", "Some error")
    # Verify it attempts to emit the failure
    mock_sanitize.assert_called()
    mock_emit.assert_called()
    # Verify even if emitting fails, the message visibility changes
    sqs_message.change_visibility.assert_called()


@patch("backend.ecs_tasks.delete_files.main.sanitize_message")
@patch("backend.ecs_tasks.delete_files.main.emit_failure_event")
def test_it_gracefully_handles_client_errors(mock_emit, mock_sanitize):
    sqs_message = MagicMock()
    mock_emit.side_effect = ClientError({}, "PutItem")
    handle_error(sqs_message, "{}", "Some error")
    # Verify it attempts to emit the failure
    mock_sanitize.assert_called()
    mock_emit.assert_called()
    # Verify even if emitting fails, the message visibility changes
    sqs_message.change_visibility.assert_called()


@patch("backend.ecs_tasks.delete_files.main.sanitize_message")
@patch("backend.ecs_tasks.delete_files.main.emit_failure_event")
def test_it_doesnt_change_message_visibility_when_rollback_fails(mock_emit, mock_sanitize):
    sqs_message = MagicMock()
    mock_emit.side_effect = ClientError({}, "DeleteObjectVersion")
    handle_error(sqs_message, "{}", "Some error", "ObjectRollbackFailed", False)
    # Verify it attempts to emit the failure
    mock_sanitize.assert_called()
    mock_emit.assert_called()
    # Verify that the visibility doesn't change for a rollback event
    sqs_message.change_visibility.assert_not_called()


@patch("backend.ecs_tasks.delete_files.main.emit_failure_event")
def test_it_gracefully_handles_change_message_visibility_failure(mock_emit):
    sqs_message = MagicMock()
    e = boto3.client("sqs").exceptions.ReceiptHandleIsInvalid
    sqs_message.change_visibility.side_effect = e({}, "ReceiptHandleIsInvalid")
    handle_error(sqs_message, "{}", "Some error")
    # Verify it attempts to emit the failure
    mock_emit.assert_called()
    sqs_message.change_visibility.assert_called()  # Implicit graceful handling


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.load_parquet")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
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
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.load_parquet")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
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
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_validates_messages_with_missing_keys(mock_error_handler):
    # Act
    execute("{}", "receipt_handle")
    # Assert
    mock_error_handler.assert_called()


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_validates_messages_with_invalid_body(mock_error_handler):
    # Act
    execute("NOT JSON", "receipt_handle")
    mock_error_handler.assert_called()


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_s3_permission_issues(mock_error_handler, mock_s3):
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.side_effect = ClientError({}, "GetObject")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    msg = mock_error_handler.call_args[0][2]
    assert msg.startswith("ClientError:")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_io_errors(mock_error_handler, mock_s3):
    # Arrange
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.side_effect = IOError("an error")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(ANY, ANY, "Unable to retrieve object: an error")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_file_too_big(mock_error_handler, mock_s3):
    # Arrange
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.side_effect = MemoryError("Too big")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(ANY, ANY, "Insufficient memory to work on object: Too big")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_generic_error(mock_error_handler, mock_s3):
    # Arrange
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.side_effect = RuntimeError("Some Error")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(ANY, ANY, "Unknown error during message processing: Some Error")


@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning")
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_unversioned_buckets(mock_error_handler, mock_s3, mock_versioning):
    # Arrange
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_versioning.side_effect = ValueError("Versioning validation Error")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(ANY, ANY, "Unprocessable message: Versioning validation Error")
    mock_versioning.assert_called_with(ANY, 'bucket')


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.load_parquet")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.save")
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
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.save", MagicMock(return_value="new_version"))
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.rollback_object_version")
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.load_parquet")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
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
    rollback_mock.assert_called_with(ANY, 'bucket', 'path/basic.parquet', 'new_version', on_error=ANY)


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.save", MagicMock(return_value="new_version"))
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.load_parquet")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
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
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.save", MagicMock(return_value="new_version"))
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.load_parquet")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
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
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.main.save", MagicMock(return_value="new_version"))
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.load_parquet")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
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


@patch("backend.ecs_tasks.delete_files.main.emit_failure_event")
def test_error_handler(mock_emit):
    msg = MagicMock()
    handle_error(msg, "{}", "Test Error")
    mock_emit.assert_called_with("{}", "Test Error", "ObjectUpdateFailed")
    msg.change_visibility.assert_called_with(VisibilityTimeout=0)


@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_kill_handler_cleans_up(mock_error_handler):
    with pytest.raises(SystemExit) as e:
        mock_pool = MagicMock()
        mock_msg = MagicMock()
        kill_handler([mock_msg], mock_pool)
        mock_pool.terminate.assert_called()
        mock_error_handler.assert_called()
        assert 1 == e.value.code


@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_kill_handler_exits_successfully_when_done(mock_error_handler):
    with pytest.raises(SystemExit) as e:
        mock_pool = MagicMock()
        kill_handler([], mock_pool)
        mock_pool.terminate.assert_called()
        mock_error_handler.assert_not_called()
        assert 0 == e.value.code


@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_gracefully_handles_cleanup_issues(mock_error_handler):
    with pytest.raises(SystemExit):
        mock_pool = MagicMock()
        mock_msg = MagicMock()
        mock_error_handler.side_effect = ValueError()
        kill_handler([mock_msg, mock_msg], mock_pool)
        assert 2 == mock_error_handler.call_count
        mock_pool.terminate.assert_called()
