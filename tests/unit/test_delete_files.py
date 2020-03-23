import os
from io import BytesIO

import boto3
from botocore.exceptions import ClientError
from mock import patch, MagicMock, mock_open, ANY

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
    emit_deletion_event, emit_failed_deletion_event, save, get_grantees, \
    get_object_info, get_object_tags, get_object_acl, get_requester_payment, get_row_count, \
    delete_from_dataframe, delete_matches_from_file, load_parquet, kill_handler, handle_error, \
    get_bucket_versioning, sanitize_message, get_object_version

pytestmark = [pytest.mark.unit]


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_version", MagicMock(return_value="abc123"))
@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
def test_happy_path_when_queue_not_empty(mock_save, mock_emit, mock_delete, mock_s3, mock_load):
    column = {"Column": "customer_id",
              "MatchIds": ["12345", "23456"]}
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = MagicMock(version_id="abc123")
    mock_load.return_value = parquet_file
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(message_stub(Object="s3://bucket/path/basic.parquet"), "receipt_handle")
    mock_s3.open.assert_called_with("s3://bucket/path/basic.parquet", "rb")
    mock_delete.assert_called_with(parquet_file, [column])
    mock_save.assert_called_with(ANY, ANY, "bucket", "path/basic.parquet", "abc123")
    mock_emit.assert_called()
    buf = mock_save.call_args[0][1]
    assert buf.read
    assert isinstance(buf, pa.BufferReader)  # must be BufferReader for zero-copy


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
@patch("backend.ecs_tasks.delete_files.delete_files.logger")
def test_warning_logged_for_no_deletions(mock_logger, mock_save, mock_emit, mock_delete, mock_s3, mock_load):
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
    emit_failed_deletion_event(msg, "Some error")
    mock_emit.assert_called_with("1234", "ObjectUpdateFailed", {
        "Error": "Some error",
        "Message": json.loads(msg)
    }, 'ECSTask_4567')


def test_it_raises_for_missing_job_id():
    with pytest.raises(ValueError):
        emit_failed_deletion_event("{}", "Some error")


@patch("backend.ecs_tasks.delete_files.delete_files.sanitize_message")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
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
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
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
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
def test_it_gracefully_handles_client_errors(mock_emit, mock_sanitize):
    sqs_message = MagicMock()
    mock_emit.side_effect = ClientError({}, "PutItem")
    handle_error(sqs_message, "{}", "Some error")
    # Verify it attempts to emit the failure
    mock_sanitize.assert_called()
    mock_emit.assert_called()
    # Verify even if emitting fails, the message visibility changes
    sqs_message.change_visibility.assert_called()


@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
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
@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_missing_col_exceptions(mock_handler, mock_delete, mock_load):
    # Arrange
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_delete.side_effect = KeyError("FAIL")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_handler.assert_called_with(ANY, ANY, "Parquet processing error: 'FAIL'")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_arrow_exceptions(mock_handler, mock_delete, mock_load):
    # Arrange
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_delete.side_effect = ArrowException("FAIL")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_handler.assert_called_with(ANY, ANY, "Parquet processing error: FAIL")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_validates_messages_with_missing_keys(mock_handler):
    # Act
    execute("{}", "receipt_handle")
    # Assert
    mock_handler.assert_called()


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_validates_messages_with_invalid_body(mock_handler):
    # Act
    execute("NOT JSON", "receipt_handle")
    mock_handler.assert_called()


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_s3_permission_issues(mock_handler, mock_s3):
    mock_s3.open.side_effect = ClientError({}, "GetObject")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    msg = mock_handler.call_args[0][2]
    assert msg.startswith("ClientError:")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_io_errors(mock_handler, mock_s3):
    # Arrange
    mock_s3.open.side_effect = IOError("an error")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_handler.assert_called_with(ANY, ANY, "Unable to retrieve object: an error")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_file_too_big(mock_handler, mock_s3):
    # Arrange
    mock_s3.open.side_effect = MemoryError("Too big")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_handler.assert_called_with(ANY, ANY, "Insufficient memory to work on object: Too big")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_generic_error(mock_handler, mock_s3):
    # Arrange
    mock_s3.open.side_effect = RuntimeError("Some Error")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_handler.assert_called_with(ANY, ANY, "Unknown error during message processing: Some Error")


@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=False))
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_handles_unversioned_buckets(mock_handler, mock_s3):
    # Arrange
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_handler.assert_called_with(ANY, ANY, "Unprocessable message: Bucket bucket does not have versioning enabled")


def test_it_returns_bucket_versioning_enabled():
    get_bucket_versioning.cache_clear()
    client = MagicMock()
    client.get_bucket_versioning.return_value = {"Status": "Enabled"}
    assert get_bucket_versioning(client, "bucket")


def test_it_returns_bucket_versioning_disabled():
    get_bucket_versioning.cache_clear()
    client = MagicMock()
    client.get_bucket_versioning.return_value = {"Status": "Suspended"}
    assert not get_bucket_versioning(client, "bucket")


def test_it_returns_latest_object_version():
    client = MagicMock()
    client.head_object.return_value = {"VersionId": "versionABC"}
    assert get_object_version(client,  "", "") == 'versionABC'


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


@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_info")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_tags")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_acl")
@patch("backend.ecs_tasks.delete_files.delete_files.get_grantees")
def test_it_applies_settings_when_saving(mock_grantees, mock_acl, mock_tagging, mock_standard, mock_requester, mock_s3):
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
    resp = save(mock_client, buf, "bucket", "key", "abc123")
    mock_file.write.assert_called_with(b'')
    assert "abc123" == resp
    mock_client.put_object_acl.assert_not_called()


@patch("backend.ecs_tasks.delete_files.delete_files.s3", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_info")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_tags")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_acl")
@patch("backend.ecs_tasks.delete_files.delete_files.get_grantees")
def test_it_passes_through_version(mock_grantees, mock_acl, mock_tagging, mock_standard, mock_requester):
    mock_client = MagicMock()
    mock_requester.return_value = {}, {}
    mock_standard.return_value = ({}, {})
    mock_tagging.return_value = ({}, {})
    mock_acl.return_value = ({}, {})
    mock_grantees.return_value = ''
    buf = BytesIO()
    save(mock_client, buf, "bucket", "key", "abc123")
    mock_acl.assert_called_with(mock_client, "bucket", "key", "abc123")
    mock_tagging.assert_called_with(mock_client, "bucket", "key", "abc123")
    mock_standard.assert_called_with(mock_client, "bucket", "key", "abc123")


@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_info")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_tags")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_acl")
@patch("backend.ecs_tasks.delete_files.delete_files.get_grantees")
def test_it_restores_write_permissions(mock_grantees, mock_acl, mock_tagging, mock_standard, mock_requester, mock_s3):
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
    save(mock_client, buf, "bucket", "key", "abc123")
    mock_client.put_object_acl.assert_called_with(
        Bucket="bucket",
        Key="key",
        VersionId="new_version123",
        GrantFullControl="id=abc",
        GrantWrite="id=123"
    )


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_version", MagicMock(return_value="abc123"))
@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
def test_it_provides_logs_for_acl_fail(mock_save, mock_handler, mock_delete, mock_load, mock_s3):
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_save.side_effect = ClientError({}, "PutObjectAcl")
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = MagicMock(version_id="abc123")
    execute(message_stub(), "receipt_handle")
    mock_save.assert_called()
    mock_handler.assert_called_with(ANY, ANY, "ClientError: An error occurred (Unknown) when calling the PutObjectAcl "
                                              "operation: Unknown. Redacted object uploaded successfully but unable to "
                                              "restore WRITE ACL")


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_version", MagicMock(return_value="version2"))
@patch("backend.ecs_tasks.delete_files.delete_files.get_bucket_versioning", MagicMock(return_value=True))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
def test_it_skip_saving_and_provides_logs_for_version_conflict(mock_save, mock_handler, mock_delete, mock_load, mock_s3):
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load.return_value = parquet_file
    mock_save.side_effect = ClientError({}, "PutObjectAcl")
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    mock_s3.open.return_value = mock_s3
    mock_s3.__enter__.return_value = MagicMock(version_id="version1")
    execute(message_stub(), "receipt_handle")
    mock_save.assert_not_called()
    mock_handler.assert_called_with(ANY, ANY, "Unprocessable message: Object versions consistency check failed. "
                                              "Race condition detected (Expected version: version1, detected: version2)")



def test_it_loads_parquet_files():
    data = [{'customer_id': '12345'}, {'customer_id': '23456'}]
    df = pd.DataFrame(data)
    buf = BytesIO()
    df.to_parquet(buf, compression="snappy")
    resp = load_parquet(buf)
    assert 2 == resp.read().num_rows


@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
def test_error_handler(mock_emit):
    msg = MagicMock()
    handle_error(msg, "{}", "Test Error")
    mock_emit.assert_called_with("{}", "Test Error")
    msg.change_visibility.assert_called_with(VisibilityTimeout=0)


@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_kill_handler_cleans_up(mock_handler):
    with pytest.raises(SystemExit) as e:
        mock_pool = MagicMock()
        mock_msg = MagicMock()
        kill_handler([mock_msg], mock_pool)
        mock_pool.terminate.assert_called()
        mock_handler.assert_called()
        assert 1 == e.value.code


@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_kill_handler_exits_successfully_when_done(mock_handler):
    with pytest.raises(SystemExit) as e:
        mock_pool = MagicMock()
        kill_handler([], mock_pool)
        mock_pool.terminate.assert_called()
        mock_handler.assert_not_called()
        assert 0 == e.value.code


@patch("backend.ecs_tasks.delete_files.delete_files.handle_error")
def test_it_gracefully_handles_cleanup_issues(mock_handler):
    with pytest.raises(SystemExit):
        mock_pool = MagicMock()
        mock_msg = MagicMock()
        mock_handler.side_effect = ValueError()
        kill_handler([mock_msg, mock_msg], mock_pool)
        assert 2 == mock_handler.call_count
        mock_pool.terminate.assert_called()


def test_it_sanitises_matches():
    assert "This message contains ID *** MATCH ID *** and *** MATCH ID ***" == sanitize_message(
        "This message contains ID 12345 and 23456", message_stub(Columns=[{
            "Column": "a", "MatchIds": ["12345", "23456", "34567"]
        }]))


def test_sanitiser_handles_malformed_messages():
    assert "an error message" == sanitize_message("an error message", "not json")


def message_stub(**kwargs):
    return json.dumps({
        "JobId": "1234",
        "Object": "s3://bucket/path/basic.parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": ["12345", "23456"]}],
        **kwargs
    })
