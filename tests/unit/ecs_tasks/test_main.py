import os
from io import BytesIO
from argparse import Namespace
from errno import ENOENT
from os import strerror

import boto3
from botocore.exceptions import ClientError
from mock import patch, MagicMock, ANY, call

import pyarrow as pa
import pytest
from pyarrow.lib import ArrowException

from s3 import DeleteOldVersionsError, IntegrityCheckFailedError

with patch.dict(
    os.environ,
    {
        "DELETE_OBJECTS_QUEUE": "https://url/q.fifo",
        "DLQ": "https://url/q",
    },
):
    from backend.ecs_tasks.delete_files.main import (
        build_matches,
        kill_handler,
        execute,
        handle_error,
        handle_skip,
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.main.save")
@patch("backend.ecs_tasks.delete_files.main.build_matches")
@patch("backend.ecs_tasks.delete_files.main.get_object_info")
def test_happy_path_when_queue_not_empty(
    mock_get_object_info,
    mock_build_matches,
    mock_save,
    mock_emit,
    mock_delete,
    mock_fs,
    mock_session,
    mock_verify_integrity,
    message_stub,
):
    column = {"Column": "customer_id", "MatchIds": set(["12345", "23456"])}
    mock_build_matches.return_value = [column]
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_file = MagicMock()
    mock_file.metadata.return_value = {
        "VersionId": b"abc123",
        "Content-Length": b"5558",
    }
    mock_fs.open_input_stream.return_value.__enter__.return_value = mock_file
    mock_get_object_info.return_value = {"Metadata": {}}, None
    mock_save.return_value = "new_version123"
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(
        "https://queue/url",
        message_stub(Object="s3://bucket/path/basic.parquet"),
        "receipt_handle",
    )
    mock_fs.open_input_stream.assert_called_with(
        "bucket/path/basic.parquet", buffer_size=5 * 2**20
    )
    mock_delete.assert_called_with(ANY, [column], "parquet", False)
    mock_save.assert_called_with(ANY, ANY, "bucket", "path/basic.parquet", {}, "abc123")
    mock_emit.assert_called()
    mock_session.assert_called_with(None, "s3f2")
    mock_verify_integrity.assert_called_with(
        ANY, "bucket", "path/basic.parquet", "abc123", "new_version123"
    )
    buf = mock_save.call_args[0][1]
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.main.save")
@patch("backend.ecs_tasks.delete_files.main.build_matches")
@patch("backend.ecs_tasks.delete_files.main.get_object_info")
def test_happy_path_when_queue_not_empty_for_compressed_json(
    mock_get_object_info,
    mock_build_matches,
    mock_save,
    mock_emit,
    mock_delete,
    mock_fs,
    mock_session,
    mock_verify_integrity,
    message_stub,
):
    column = {"Column": "customer_id", "MatchIds": set(["12345", "23456"])}
    mock_build_matches.return_value = [column]
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_file = MagicMock()
    mock_file.metadata.return_value = {
        "VersionId": b"abc123",
        "Content-Length": b"5558",
    }
    mock_fs.open_input_stream.return_value.__enter__.return_value = mock_file
    mock_get_object_info.return_value = {"Metadata": {}}, None
    mock_save.return_value = "new_version123"
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(
        "https://queue/url",
        message_stub(Object="s3://bucket/path/basic.json.gz", Format="json"),
        "receipt_handle",
    )
    mock_fs.open_input_stream.assert_called_with(
        "bucket/path/basic.json.gz", buffer_size=5 * 2**20
    )
    mock_delete.assert_called_with(ANY, [column], "json", True)
    mock_save.assert_called_with(ANY, ANY, "bucket", "path/basic.json.gz", {}, "abc123")
    mock_emit.assert_called()
    mock_session.assert_called_with(None, "s3f2")
    mock_verify_integrity.assert_called_with(
        ANY, "bucket", "path/basic.json.gz", "abc123", "new_version123"
    )
    buf = mock_save.call_args[0][1]
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.main.save")
@patch("backend.ecs_tasks.delete_files.main.build_matches")
@patch("backend.ecs_tasks.delete_files.main.is_kms_cse_encrypted")
@patch("backend.ecs_tasks.delete_files.main.encrypt")
@patch("backend.ecs_tasks.delete_files.main.decrypt")
@patch("backend.ecs_tasks.delete_files.main.get_object_info")
def test_cse_kms_encrypted(
    mock_get_object_info,
    mock_decrypt,
    mock_encrypt,
    mock_is_encrypted,
    mock_build_matches,
    mock_save,
    mock_emit,
    mock_delete,
    mock_fs,
    mock_session,
    mock_verify_integrity,
    message_stub,
):
    column = {"Column": "customer_id", "MatchIds": set(["12345", "23456"])}
    metadata = {"x-amz-wrap-alg": "kms", "x-amz-key-v2": "key123"}
    mock_build_matches.return_value = [column]
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_file = MagicMock()
    mock_file.metadata.return_value = {
        "VersionId": b"abc123",
        "Content-Length": b"5558",
    }
    mock_fs.open_input_stream.return_value.__enter__.return_value = mock_file
    mock_get_object_info.return_value = {"Metadata": metadata}, None
    mock_save.return_value = "new_version123"
    mock_file_decrypted = BytesIO(b"")
    mock_is_encrypted.return_value = True
    redacted = pa.BufferOutputStream()
    redacted_encrypted = BytesIO(b"")
    mock_delete.return_value = redacted, {"DeletedRows": 1}
    mock_decrypt.return_value = mock_file_decrypted
    mock_encrypt.return_value = redacted_encrypted, {"new_metadata": "foo"}
    execute(
        "https://queue/url",
        message_stub(Object="s3://bucket/path/basic.parquet"),
        "receipt_handle",
    )
    mock_is_encrypted.assert_called_with(metadata)
    mock_decrypt.assert_called_with(mock_file, metadata, ANY)
    mock_fs.open_input_stream.assert_called_with(
        "bucket/path/basic.parquet", buffer_size=5 * 2**20
    )
    mock_delete.assert_called_with(mock_file_decrypted, [column], "parquet", False)
    mock_encrypt.assert_called_with(ANY, metadata, ANY)
    mock_save.assert_called_with(
        ANY,
        redacted_encrypted,
        "bucket",
        "path/basic.parquet",
        {"new_metadata": "foo"},
        "abc123",
    )
    mock_emit.assert_called()
    mock_session.assert_called_with(None, "s3f2")
    mock_verify_integrity.assert_called_with(
        ANY, "bucket", "path/basic.parquet", "abc123", "new_version123"
    )


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
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session")
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.get_object_info")
def test_it_assumes_role(
    mock_get_object_info, mock_delete, mock_fs, mock_session, message_stub
):
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_file = MagicMock()
    mock_file.metadata.return_value = {
        "VersionId": b"abc123",
        "Content-Length": b"5558",
    }
    mock_fs.open_input_stream.return_value.__enter__.return_value = mock_file
    mock_get_object_info.return_value = {"Metadata": {}}, None
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(
        "https://queue/url",
        message_stub(
            RoleArn="arn:aws:iam:account_id:role/rolename",
            Object="s3://bucket/path/basic.parquet",
        ),
        "receipt_handle",
    )
    mock_session.assert_called_with("arn:aws:iam:account_id:role/rolename", "s3f2")


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
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_object_info")
def test_it_removes_old_versions(
    mock_get_object_info,
    mock_delete,
    mock_fs,
    mock_delete_versions,
    mock_save,
    message_stub,
):
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_file = MagicMock()
    mock_file.metadata.return_value = {
        "VersionId": b"abc123",
        "Content-Length": b"5558",
    }
    mock_fs.open_input_stream.return_value.__enter__.return_value = mock_file
    mock_get_object_info.return_value = {"Metadata": {}}, None
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_object_info")
def test_it_handles_old_version_delete_failures(
    mock_get_object_info,
    mock_handle,
    mock_delete,
    mock_fs,
    mock_delete_versions,
    mock_save,
    message_stub,
):
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_file = MagicMock()
    mock_file.metadata.return_value = {
        "VersionId": b"abc123",
        "Content-Length": b"5558",
    }
    mock_fs.open_input_stream.return_value.__enter__.return_value = mock_file
    mock_get_object_info.return_value = {"Metadata": {}}, None
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.main.save")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_object_info")
def test_it_handles_no_deletions(
    mock_get_object_info,
    mock_handle,
    mock_save,
    mock_emit,
    mock_delete,
    mock_fs,
    message_stub,
):
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_file = MagicMock()
    mock_file.metadata.return_value = {
        "VersionId": b"abc123",
        "Content-Length": b"5558",
    }
    mock_fs.open_input_stream.return_value.__enter__.return_value = mock_file
    mock_get_object_info.return_value = {"Metadata": {}}, None
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 0}
    execute(
        "https://queue/url",
        message_stub(Object="s3://bucket/path/basic.parquet"),
        "receipt_handle",
    )
    mock_fs.open_input_stream.assert_called_with(
        "bucket/path/basic.parquet", buffer_size=5 * 2**20
    )
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.build_matches")
def test_it_handles_missing_col_exceptions(
    mock_build_matches, mock_error_handler, mock_delete, message_stub
):
    # Arrange
    mock_delete.side_effect = KeyError("FAIL")
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(
        ANY, ANY, "Apache Arrow processing error: 'FAIL'"
    )


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.pa.fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
def test_it_handles_arrow_exceptions(mock_error_handler, mock_delete, message_stub):
    # Arrange
    mock_delete.side_effect = ArrowException("FAIL")
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(
        ANY, ANY, "Apache Arrow processing error: FAIL"
    )


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
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_s3_permission_issues(mock_error_handler, mock_fs, message_stub):
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_fs.open_input_stream.side_effect = ClientError({}, "GetObject")
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
def test_it_handles_io_errors(mock_error_handler, mock_fs, message_stub):
    # Arrange
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_fs.open_input_stream.side_effect = IOError("an error")
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
def test_it_handles_file_too_big(mock_error_handler, mock_fs, message_stub):
    # Arrange
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_fs.open_input_stream.side_effect = MemoryError("Too big")
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(
        ANY, ANY, "Insufficient memory to work on object: Too big"
    )


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_does_not_ignore_not_found_error_by_default(
    mock_error_handler, mock_fs, message_stub
):
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_fs.open_input_stream.side_effect = ClientError(
        {"Error": {"Code": "404"}}, "HeadObject"
    )
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    msg = mock_error_handler.call_args[0][2]
    assert msg.startswith("ClientError:")


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.handle_skip")
@patch("backend.ecs_tasks.delete_files.main.get_object_info")
def test_it_ignores_boto_not_found_error_if_param_is_true(
    mock_get_object_info, mock_skip_handler, mock_error_handler, mock_fs, message_stub
):
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_get_object_info.side_effect = ClientError(
        {"Error": {"Code": "404"}}, "HeadObject"
    )
    # Act
    execute(
        "https://queue/url",
        message_stub(IgnoreObjectNotFoundExceptions=True),
        "receipt_handle",
    )
    # Assert
    mock_error_handler.assert_not_called()
    msg = mock_skip_handler.call_args[0][2]
    assert msg.startswith("Ignored error: ClientError:")


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.handle_skip")
def test_it_ignores_arrow_not_found_error_if_param_is_true(
    mock_skip_handler, mock_error_handler, mock_fs, message_stub
):
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_fs.open_input_stream.side_effect = FileNotFoundError(
        ENOENT, strerror(ENOENT), "bucket/key"
    )
    # Act
    execute(
        "https://queue/url",
        message_stub(IgnoreObjectNotFoundExceptions=True),
        "receipt_handle",
    )
    # Assert
    mock_error_handler.assert_not_called()
    msg = mock_skip_handler.call_args[0][2]
    assert msg.startswith("Ignored error: Apache Arrow S3FileSystem Error:")


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
def test_it_handles_not_found_error(mock_error_handler, mock_fs, message_stub):
    # Arrange
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_fs.open_input_stream.side_effect = FileNotFoundError(
        ENOENT, strerror(ENOENT), "bucket/key"
    )
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(
        ANY,
        ANY,
        "Apache Arrow S3FileSystem Error: [Errno 2] No such file or directory: 'bucket/key'",
    )


@patch.dict(os.environ, {"JobTable": "test"})
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch(
    "backend.ecs_tasks.delete_files.main.validate_bucket_versioning",
    MagicMock(return_value=True),
)
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
def test_it_handles_generic_error(mock_error_handler, mock_fs, message_stub):
    # Arrange
    mock_fs.S3FileSystem.return_value = mock_fs
    mock_fs.open_input_stream.side_effect = RuntimeError("Some Error")
    # Act
    execute("https://queue/url", message_stub(), "receipt_handle")
    # Assert
    mock_error_handler.assert_called_with(
        ANY, ANY, "Unknown error during message processing: Some Error"
    )


@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.validate_bucket_versioning")
@patch("backend.ecs_tasks.delete_files.main.pa.fs")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
def test_it_handles_unversioned_buckets(
    mock_error_handler, mock_fs, mock_versioning, message_stub
):
    # Arrange
    mock_fs.S3FileSystem.return_value = mock_fs
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.save")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.rollback_object_version")
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
def test_it_provides_logs_for_failed_version_integrity_check_and_performs_rollback(
    mock_error_handler,
    mock_delete,
    mock_verify_integrity,
    rollback_mock,
    message_stub,
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
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
@patch("backend.ecs_tasks.delete_files.main.pa.fs", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_session", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.get_queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.main.verify_object_versions_integrity")
@patch("backend.ecs_tasks.delete_files.main.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.main.handle_error")
@patch("backend.ecs_tasks.delete_files.main.build_matches", MagicMock())
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


@patch("backend.ecs_tasks.delete_files.main.sanitize_message")
@patch("backend.ecs_tasks.delete_files.main.emit_skipped_event")
def test_skip_handler(mock_emit, mock_sanitize):
    sqs_message = MagicMock()
    handle_skip(
        sqs_message, {"Object": "s3://bucket/path/basic.parquet"}, "Ignored error"
    )
    # Verify it deletes the message
    sqs_message.delete.assert_called()
    # Verify it emits the skip event
    mock_sanitize.assert_called()
    mock_emit.assert_called()


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
@patch.dict(os.environ, {"AWS_URL_SUFFIX": "amazonaws.com"})
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
@patch("backend.ecs_tasks.delete_files.main.get_context")
@patch("backend.ecs_tasks.delete_files.main.get_queue")
def test_it_starts_subprocesses(mock_queue, mock_context):
    mock_queue.return_value = mock_queue
    mock_message = MagicMock()
    mock_queue.receive_messages.return_value = [mock_message]
    # Break out of while loop
    mock_pool = MagicMock()
    mock_context.return_value = mock_context
    mock_context.__enter__.return_value = mock_context
    mock_context.Pool = mock_pool
    mock_pool.return_value = mock_pool
    mock_pool.__enter__.return_value = mock_pool
    mock_pool.starmap.side_effect = RuntimeError("Break loop")
    with pytest.raises(RuntimeError):
        main("https://queue/url", 1, 1, 1)
    mock_pool.assert_called_with(maxtasksperchild=1)
    mock_context.assert_called_with("spawn")
    mock_pool.starmap.assert_called_with(
        ANY, [("https://queue/url", mock_message.body, mock_message.receipt_handle)]
    )
    mock_queue.receive_messages.assert_called_with(
        WaitTimeSeconds=1, MaxNumberOfMessages=1
    )


@patch("backend.ecs_tasks.delete_files.main.get_context", MagicMock())
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


@patch("backend.ecs_tasks.delete_files.main.get_context", MagicMock())
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


@patch("backend.ecs_tasks.delete_files.main.fetch_manifest")
def test_it_builds_matches_grouped_by_column_simple(mock_fetch):
    cols = [{"Column": "customer_id"}]
    mock_fetch.return_value = (
        '{"Columns":["customer_id"], "MatchId": ["12345"], "QueryableColumns": "customer_id"}\n'
        '{"Columns":["customer_id"], "MatchId": ["23456"], "QueryableColumns": "customer_id"}\n'
    )

    matches = build_matches(cols, "s3://path-to-manifest.json")
    assert matches == [
        {"Column": "customer_id", "MatchIds": set(["12345", "23456"])},
    ]


@patch("backend.ecs_tasks.delete_files.main.fetch_manifest")
def test_it_builds_matches_grouped_by_column_composite(mock_fetch):
    cols = [
        {"Columns": ["first_name", "last_name"]},
    ]
    mock_fetch.return_value = (
        '{"Columns":["first_name", "last_name"], "MatchId": ["john", "doe"], "QueryableColumns": "first_name_S3F2COMP_last_name"}\n'
        '{"Columns":["first_name", "last_name"], "MatchId": ["jane", "doe"], "QueryableColumns": "first_name_S3F2COMP_last_name"}\n'
    )

    matches = build_matches(cols, "s3://path-to-manifest.json")
    assert matches == [
        {
            "Columns": ["first_name", "last_name"],
            "MatchIds": set([tuple(["john", "doe"]), tuple(["jane", "doe"])]),
        },
    ]


@patch("backend.ecs_tasks.delete_files.main.fetch_manifest")
def test_it_builds_matches_grouped_by_column_mixed(mock_fetch):
    # example in which first_name and last_name are the col identifiers for given Data Mapper
    cols = [
        {"Columns": ["first_name", "last_name"]},
        {"Column": "first_name"},
        {"Column": "last_name"},
    ]
    # Simple => "smith" value to be searched in any column, Composite => particular tuples or single value ("parker")
    mock_fetch.return_value = (
        '{"Columns":["first_name", "last_name"], "MatchId": ["john", "doe"], "QueryableColumns": "first_name_S3F2COMP_last_name"}\n'
        '{"Columns":["first_name", "last_name"], "MatchId": ["jane", "doe"], "QueryableColumns": "first_name_S3F2COMP_last_name"}\n'
        '{"Columns":["first_name"], "MatchId": ["smith"], "QueryableColumns": "first_name"}\n'
        '{"Columns":["last_name"], "MatchId": ["smith"], "QueryableColumns": "last_name"}\n'
        '{"Columns":["last_name"], "MatchId": ["parker"], "QueryableColumns": "last_name"}\n'
    )

    matches = build_matches(cols, "s3://path-to-manifest.json")
    assert matches == [
        {
            "Columns": ["first_name", "last_name"],
            "MatchIds": set([tuple(["john", "doe"]), tuple(["jane", "doe"])]),
        },
        {"Column": "first_name", "MatchIds": set(["smith"])},
        {"Column": "last_name", "MatchIds": set(["smith", "parker"])},
    ]
