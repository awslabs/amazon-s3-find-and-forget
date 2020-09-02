import os
from argparse import Namespace

import boto3
from botocore.exceptions import ClientError
from mock import patch, MagicMock, ANY, call

import pyarrow as pa
import pytest
from pyarrow.lib import ArrowException

from s3 import DeleteOldVersionsError, IntegrityCheckFailedError

with patch.dict(
    os.environ, {"DELETE_OBJECTS_QUEUE": "https://url/q.fifo", "DLQ": "https://url/q",}
):
    from backend.ecs_tasks.delete_files.main import (
        kill_handler,
        execute,
        handle_error,
        get_queue,
        main,
        parse_args,
        delete_matches_from_file,
    )

pytestmark = [pytest.mark.unit, pytest.mark.ecs_tasks]


def get_list_object_versions_error():
    return ClientError(
        {
            "Error": {
                "Code": "InvalidArgument",
                "Message": "Invalid version id specified",
            }
        },
        "ListObjectVersions",
    )


@patch.dict(os.environ, {"JobTable": "test"})
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.get_session")
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.main.save")
def test_happy_path_when_queue_not_empty(
    mock_save,
    mock_emit,
    mock_delete,
    mock_s3,
    mock_session,
    mock_verify_integrity,
    message_stub,
):
    mock_s3.S3FileSystem.return_value = mock_s3
    column = {"Column": "customer_id", "MatchIds": ["12345", "23456"]}
    mock_file = MagicMock(version_id="abc123")
    mock_save.return_value = "new_version123"
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = mock_file
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(
        "https://queue/url",
        message_stub(Object="s3://bucket/path/basic.parquet"),
        "receipt_handle",
    )
    mock_s3.open.assert_called_with("s3://bucket/path/basic.parquet", "rb")
    mock_delete.assert_called_with(mock_file, [column], "parquet", False)
    mock_save.assert_called_with(
        ANY, ANY, ANY, "bucket", "path/basic.parquet", "abc123"
    )
    mock_emit.assert_called()
    mock_session.assert_called_with(None)
    mock_verify_integrity.assert_called_with(
        ANY, "bucket", "path/basic.parquet", "abc123", "new_version123"
    )
    buf = mock_save.call_args[0][2]
    assert buf.read
    assert isinstance(buf, pa.BufferReader)  # must be BufferReader for zero-copy


@patch.dict(os.environ, {"JobTable": "test"})
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.get_session")
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.main.save")
def test_happy_path_when_queue_not_empty_for_compressed_json(
    mock_save,
    mock_emit,
    mock_delete,
    mock_s3,
    mock_session,
    mock_verify_integrity,
    message_stub,
):
    mock_s3.S3FileSystem.return_value = mock_s3
    column = {"Column": "customer_id", "MatchIds": ["12345", "23456"]}
    mock_file = MagicMock(version_id="abc123")
    mock_save.return_value = "new_version123"
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = mock_file
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(
        "https://queue/url",
        message_stub(Object="s3://bucket/path/basic.json.gz", Format="json"),
        "receipt_handle",
    )
    mock_s3.open.assert_called_with("s3://bucket/path/basic.json.gz", "rb")
    mock_delete.assert_called_with(mock_file, [column], "json", True)
    mock_save.assert_called_with(
        ANY, ANY, ANY, "bucket", "path/basic.json.gz", "abc123"
    )
    mock_emit.assert_called()
    mock_session.assert_called_with(None)
    mock_verify_integrity.assert_called_with(
        ANY, "bucket", "path/basic.json.gz", "abc123", "new_version123"
    )
    buf = mock_save.call_args[0][2]
    assert buf.read
    assert isinstance(buf, pa.BufferReader)  # must be BufferReader for zero-copy


@patch.dict(os.environ, {"JobTable": "test"})
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch(
    "backend.ecs_tasks.delete_files.main.verify_object_versions_integrity",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.save", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session")
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
def test_it_assumes_role(mock_delete, mock_s3, mock_session, message_stub):
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = MagicMock(version_id="abc123")
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(
        "https://queue/url",
        message_stub(
            RoleArn="arn:aws:iam:account_id:role/rolename",
            Object="s3://bucket/path/basic.parquet",
        ),
        "receipt_handle",
    )
    mock_session.assert_called_with("arn:aws:iam:account_id:role/rolename")


@patch.dict(os.environ, {"JobTable": "test"})
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch(
    "backend.ecs_tasks.delete_files.main.verify_object_versions_integrity",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.save")
@patch("backend.ecs_tasks.delete_files.main.delete_old_versions")
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
def test_it_removes_old_versions(
    mock_delete, mock_s3, mock_delete_versions, mock_save, message_stub
):
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = MagicMock(version_id="abc123")
    mock_save.return_value = "new_version123"
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(
        "https://queue/url",
        message_stub(
            RoleArn="arn:aws:iam:account_id:role/rolename",
            DeleteOldVersions=True,
            Object="s3://bucket/path/basic.parquet",
        ),
        "receipt_handle",
    )
    mock_delete_versions.assert_called_with(ANY, ANY, ANY, "new_version123")


@patch.dict(os.environ, {"JobTable": "test"})
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch(
    "backend.ecs_tasks.delete_files.main.verify_object_versions_integrity",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.save")
@patch("backend.ecs_tasks.delete_files.main.delete_old_versions")
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_old_version_delete_failures(
    mock_handle, mock_delete, mock_s3, mock_delete_versions, mock_save, message_stub,
):
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = MagicMock(version_id="abc123")
    mock_save.return_value = "new_version123"
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    mock_delete_versions.side_effect = DeleteOldVersionsError(errors=["access denied"])
    execute(
        "https://queue/url",
        message_stub(
            RoleArn="arn:aws:iam:account_id:role/rolename",
            DeleteOldVersions=True,
            Object="s3://bucket/path/basic.parquet",
        ),
        "receipt_handle",
    )
    mock_handle.assert_called_with(
        ANY, ANY, "Unable to delete previous versions: access denied"
    )


@patch.dict(os.environ, {"JobTable": "test"})
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.main.save")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_no_deletions(
    mock_handle, mock_save, mock_emit, mock_delete, mock_s3, message_stub
):
    mock_s3.S3FileSystem.return_value = mock_s3
    column = {"Column": "customer_id", "MatchIds": ["12345", "23456"]}
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 0}
    execute(
        "https://queue/url",
        message_stub(Object="s3://bucket/path/basic.parquet"),
        "receipt_handle",
    )
    mock_s3.open.assert_called_with("s3://bucket/path/basic.parquet", "rb")
    mock_save.assert_not_called()
    mock_emit.assert_not_called()
    mock_handle.assert_called_with(
        ANY,
        ANY,
        "Unprocessable message: The object s3://bucket/path/basic.parquet "
        "was processed successfully but no rows required deletion",
    )


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_missing_col_exceptions(
    mock_error_handler, mock_delete, message_stub
):
    # Arrange
    mock_delete.side_effect = KeyError("FAIL")
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(ANY, ANY, "Parquet processing error: 'FAIL'")


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_arrow_exceptions(mock_error_handler, mock_delete, message_stub):
    # Arrange
    mock_delete.side_effect = ArrowException("FAIL")
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(ANY, ANY, "Parquet processing error: FAIL")


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_validates_messages_with_missing_keys(mock_error_handler):
    # Act
    execute("https://queue/url", "{}", "receipt_handle")
    # Assert
    mock_error_handler.assert_called()


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_validates_messages_with_invalid_body(mock_error_handler):
    # Act
    execute("https://queue/url", "NOT JSON", "receipt_handle")
    mock_error_handler.assert_called()


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_s3_permission_issues(mock_error_handler, mock_s3, message_stub):
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.side_effect = ClientError({}, "GetObject")
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    msg = mock_error_handler.call_args[0][2]
    assert msg.startswith("ClientError:")


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_io_errors(mock_error_handler, mock_s3, message_stub):
    # Arrange
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.side_effect = IOError("an error")
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(
        ANY, ANY, "Unable to retrieve object: an error"
    )


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_file_too_big(mock_error_handler, mock_s3, message_stub):
    # Arrange
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.side_effect = MemoryError("Too big")
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(
        ANY, ANY, "Insufficient memory to work on object: Too big"
    )


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_generic_error(mock_error_handler, mock_s3, message_stub):
    # Arrange
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_s3.open.side_effect = RuntimeError("Some Error")
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(
        ANY, ANY, "Unknown error during message processing: Some Error"
    )


@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning")
@patch("backend.ecs_tasks.delete_files.main.s3fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_unversioned_buckets(
    mock_error_handler, mock_s3, mock_versioning, message_stub
):
    # Arrange
    mock_s3.S3FileSystem.return_value = mock_s3
    mock_versioning.side_effect = ValueError("Versioning validation Error")
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(
        ANY, ANY, "Unprocessable message: Versioning validation Error"
    )
    mock_versioning.assert_called_with(ANY, "bucket")


@patch.dict(os.environ, {"JobTable": "test"})
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.save")
def test_it_provides_logs_for_acl_fail(
    mock_save, mock_error_handler, mock_delete, message_stub
):
    mock_save.side_effect = ClientError({}, "PutObjectAcl")
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute("https://queue/url", message_stub(), "receipt_handle")
    mock_save.assert_called()
    mock_error_handler.assert_called_with(
        ANY,
        ANY,
        "ClientError: An error occurred (Unknown) when calling the PutObjectAcl "
        "operation: Unknown. Redacted object uploaded successfully but unable to "
        "restore WRITE ACL",
    )


@patch.dict(os.environ, {"JobTable": "test"})
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch(
    "backend.ecs_tasks.delete_files.main.save", MagicMock(return_value="new_version")
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.rollback_object_version")
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_provides_logs_for_failed_version_integrity_check_and_performs_rollback(
    mock_error_handler, mock_delete, mock_verify_integrity, rollback_mock, message_stub,
):
    mock_verify_integrity.side_effect = IntegrityCheckFailedError(
        "Some error", MagicMock(), "bucket", "path/basic.parquet", "new_version"
    )

    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute("https://queue/url", message_stub(), "receipt_handle")
    mock_verify_integrity.assert_called()
    mock_error_handler.assert_called_with(
        ANY, ANY, "Object version integrity check failed: Some error"
    )
    rollback_mock.assert_called_with(
        ANY, "bucket", "path/basic.parquet", "new_version", on_error=ANY
    )


@patch.dict(os.environ, {"JobTable": "test"})
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch(
    "backend.ecs_tasks.delete_files.main.save", MagicMock(return_value="new_version")
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_provides_logs_for_get_latest_version_fail(
    mock_error_handler, mock_delete, mock_verify_integrity, message_stub
):
    mock_verify_integrity.side_effect = get_list_object_versions_error()
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute("https://queue/url", message_stub(), "receipt_handle")
    mock_verify_integrity.assert_called()
    mock_error_handler.assert_called_with(
        ANY,
        ANY,
        "ClientError: An error occurred (InvalidArgument) when calling the "
        "ListObjectVersions operation: Invalid version id specified. Could "
        "not verify redacted object version integrity",
    )


@patch.dict(os.environ, {"JobTable": "test"})
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch(
    "backend.ecs_tasks.delete_files.main.save", MagicMock(return_value="new_version")
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_provides_logs_for_failed_rollback_client_error(
    mock_error_handler, mock_delete, mock_verify_integrity, message_stub
):
    mock_s3 = MagicMock()
    mock_s3.delete_object.side_effect = ClientError({}, "DeleteObject")
    mock_verify_integrity.side_effect = IntegrityCheckFailedError(
        "Some error", mock_s3, "bucket", "test/basic.parquet", "new_version"
    )
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute("https://queue/url", message_stub(), "receipt_handle")
    mock_verify_integrity.assert_called()
    assert mock_error_handler.call_args_list == [
        call(ANY, ANY, "Object version integrity check failed: Some error"),
        call(
            ANY,
            ANY,
            "ClientError: An error occurred (Unknown) when calling the DeleteObject operation: Unknown. "
            "Version rollback caused by version integrity conflict failed",
            "ObjectRollbackFailed",
            False,
        ),
    ]


@patch.dict(os.environ, {"JobTable": "test"})
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch(
    "backend.ecs_tasks.delete_files.main.save", MagicMock(return_value="new_version")
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.s3fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_provides_logs_for_failed_rollback_generic_error(
    mock_error_handler, mock_delete, mock_verify_integrity, message_stub
):
    mock_s3 = MagicMock()
    mock_s3.delete_object.side_effect = Exception("error!!")
    mock_verify_integrity.side_effect = IntegrityCheckFailedError(
        "Some error", mock_s3, "bucket", "test/basic.parquet", "new_version"
    )
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute("https://queue/url", message_stub(), "receipt_handle")
    mock_verify_integrity.assert_called()
    assert mock_error_handler.call_args_list == [
        call(ANY, ANY, "Object version integrity check failed: Some error"),
        call(
            ANY,
            ANY,
            "Unknown error: error!!. Version rollback caused by version integrity conflict failed",
            "ObjectRollbackFailed",
            False,
        ),
    ]


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
def test_it_doesnt_change_message_visibility_when_rollback_fails(
    mock_emit, mock_sanitize
):
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
    sqs_message.meta.client.exceptions.MessageNotInflight = e
    sqs_message.meta.client.exceptions.ReceiptHandleIsInvalid = e
    sqs_message.change_visibility.side_effect = e({}, "ReceiptHandleIsInvalid")
    handle_error(sqs_message, "{}", "Some error")
    # Verify it attempts to emit the failure
    mock_emit.assert_called()
    sqs_message.change_visibility.assert_called()  # Implicit graceful handling


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


@patch.dict(os.environ, {"DELETE_OBJECTS_QUEUE": "https://queue/url"})
def test_it_inits_arg_parser_with_defaults():
    res = parse_args([])
    assert isinstance(res, Namespace)
    assert all(
        [
            hasattr(res, attr)
            for attr in ["wait_time", "max_messages", "sleep_time", "queue_url"]
        ]
    )
    assert isinstance(res.wait_time, int)
    assert isinstance(res.max_messages, int)
    assert isinstance(res.sleep_time, int)
    assert isinstance(res.queue_url, str)


@patch("backend.ecs_tasks.delete_files.main.boto3")
@patch.dict(os.environ, {"AWS_DEFAULT_REGION": "eu-west-2"})
def test_it_inits_queue_with_regional_url(mock_boto):
    get_queue("https://queue/rule")
    mock_boto.resource.assert_called_with(
        "sqs", endpoint_url="https://sqs.eu-west-2.amazonaws.com"
    )


@patch("backend.ecs_tasks.delete_files.main.boto3")
@patch("os.getenv", MagicMock(return_value=None))
def test_it_uses_default_if_region_not_in_env(mock_boto):
    get_queue("https://queue/rule")
    mock_boto.resource.assert_called_with("sqs")


@patch("backend.ecs_tasks.delete_files.main.boto3")
def test_it_does_not_override_user_supplied_endpoint_url(mock_boto):
    get_queue("https://queue/rule", endpoint_url="https://my/url")
    mock_boto.resource.assert_called_with("sqs", endpoint_url="https://my/url")


@patch("backend.ecs_tasks.delete_files.main.signal", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.Pool")
@patch("backend.ecs_tasks.delete_files.main.get_queue")
def test_it_starts_subprocesses(mock_queue, mock_pool):
    mock_queue.return_value = mock_queue
    mock_message = MagicMock()
    mock_queue.receive_messages.return_value = [mock_message]
    # Break out of while loop
    mock_pool.return_value = mock_pool
    mock_pool.__enter__.return_value = mock_pool
    mock_pool.starmap.side_effect = RuntimeError("Break loop")
    with pytest.raises(RuntimeError):
        main("https://queue/url", 1, 1, 1)
    mock_pool.assert_called_with(maxtasksperchild=1)
    mock_pool.starmap.assert_called_with(
        ANY, [("https://queue/url", mock_message.body, mock_message.receipt_handle)]
    )
    mock_queue.receive_messages.assert_called_with(
        WaitTimeSeconds=1, MaxNumberOfMessages=1
    )


@patch("backend.ecs_tasks.delete_files.main.Pool", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.signal", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue")
@patch("backend.ecs_tasks.delete_files.main.time")
def test_it_sleeps_where_no_messages(mock_time, mock_queue):
    mock_queue.return_value = mock_queue
    mock_queue.receive_messages.return_value = []
    # Break out of while loop
    mock_time.sleep.side_effect = RuntimeError("Break Loop")
    with pytest.raises(RuntimeError):
        main("https://queue/url", 1, 1, 1)
    mock_time.sleep.assert_called_with(1)


@patch("backend.ecs_tasks.delete_files.main.Pool", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.signal")
@patch("backend.ecs_tasks.delete_files.main.get_queue")
def test_it_sets_kill_handlers(mock_queue, mock_signal):
    mock_queue.return_value = mock_queue
    # Break out of while loop
    mock_queue.receive_messages.side_effect = RuntimeError("Break Loop")
    with pytest.raises(RuntimeError):
        main("https://queue/url", 1, 1, 1)
    assert mock_signal.SIGINT, ANY == mock_signal.signal.call_args_list[0][0]
    assert mock_signal.SIGTERM, ANY == mock_signal.signal.call_args_list[1][0]


@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_json_file")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_parquet_file")
def test_it_deletes_from_json_file(mock_parquet, mock_json):
    f = MagicMock()
    cols = MagicMock()
    delete_matches_from_file(f, cols, "json", False)
    mock_json.assert_called_with(f, cols, False)
    mock_parquet.assert_not_called()


@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_json_file")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_parquet_file")
def test_it_deletes_from_parquet_file(mock_parquet, mock_json):
    f = MagicMock()
    cols = MagicMock()
    delete_matches_from_file(f, cols, "parquet")
    mock_parquet.assert_called_with(f, cols)
    mock_json.assert_not_called()
