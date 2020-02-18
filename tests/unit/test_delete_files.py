import os
from io import BytesIO

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
    "SAFE_MODE_BUCKET": "test",
    "SAFE_MODE_PREFIX": "results/",
}):
    from backend.ecs_tasks.delete_files.delete_files import execute, get_container_id, \
    emit_deletion_event, emit_failed_deletion_event, save, get_grantees, \
    get_object_info, get_object_tags, get_object_acl, get_requester_payment, safe_mode, get_row_count, \
    delete_from_dataframe, delete_matches_from_file, get_object_as_parquet_file

pytestmark = [pytest.mark.unit]


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.safe_mode", MagicMock(return_value=False))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_as_parquet_file")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
def test_happy_path_when_queue_not_empty(mock_save, mock_emit, mock_delete, mock_get):
    column = {"Column": "customer_id",
              "MatchIds": ["12345", "23456"]}
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_get.return_value = parquet_file
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(message_stub(Object="s3://bucket/path/basic.parquet"), "receipt_handle")
    mock_get.assert_called_with(ANY, "bucket", "path/basic.parquet")
    mock_delete.assert_called_with(parquet_file, [column])
    mock_save.assert_called_with(ANY, ANY, "bucket", "path/basic.parquet", False)
    mock_emit.assert_called()
    buf = mock_save.call_args[0][1]
    assert buf.read
    assert isinstance(buf, pa.BufferReader)  # must be BufferReader for zero-copy


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.safe_mode", MagicMock(return_value=False))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_as_parquet_file")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
@patch("backend.ecs_tasks.delete_files.delete_files.logger")
def test_warning_logged_for_no_deletions(mock_logger, mock_save, mock_emit, mock_delete, mock_get):
    column = {"Column": "customer_id",
              "MatchIds": ["12345", "23456"]}
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_get.return_value = parquet_file
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 0}
    execute(message_stub(Object="s3://bucket/path/basic.parquet"), "receipt_handle")
    mock_get.assert_called_with(ANY, "bucket", "path/basic.parquet")
    mock_delete.assert_called_with(parquet_file, [column])
    mock_save.assert_not_called()
    mock_logger.warning.assert_called()
    mock_emit.assert_called()


def test_it_converts_s3_to_parquet_file():
    data = [{'customer_id': '12345'}]
    df = pd.DataFrame(data)
    mock_resource = MagicMock()
    mock_resource.Object.return_value = mock_resource
    mock_resource.download_fileobj.side_effect = lambda buf: df.to_parquet(buf)
    res = get_object_as_parquet_file(mock_resource, "bucket", "key")
    assert isinstance(res, pq.ParquetFile)
    assert 1 == res.read().num_rows
    assert 1 == len(res.read().columns)
    assert ["12345"] == list(res.read().column("customer_id"))


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


@patch("backend.ecs_tasks.delete_files.delete_files.emit_event")
def test_it_throws_if_no_job_id_in_emit(mock_emit):
    with pytest.raises(ValueError):
        emit_failed_deletion_event({}, "Some error")
        mock_emit.assert_not_called()


@patch("os.getenv", MagicMock(return_value="/some/path"))
@patch("os.path.isfile", MagicMock(return_value=True))
def test_it_loads_container_id_from_metadata():
    get_container_id.cache_clear()
    with patch("builtins.open", mock_open(read_data="{\"ContainerId\": \"123\"}")):
        resp = get_container_id()
        assert "123" == resp


@patch("os.getenv", MagicMock(return_value=None))
@patch("backend.ecs_tasks.delete_files.delete_files.uuid4")
def test_it_provides_default_id(mock_uuid):
    get_container_id.cache_clear()
    mock_uuid.return_value = "123"
    resp = get_container_id()
    assert "123" == resp


def test_it_returns_uuid_as_string():
    get_container_id.cache_clear()
    resp = get_container_id()
    assert isinstance(resp, str)


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.safe_mode", MagicMock(return_value=False))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_as_parquet_file")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
def test_it_handles_missing_col_exceptions(mock_queue, mock_emit, mock_delete, mock_get):
    # Arrange
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_get.return_value = parquet_file
    mock_delete.side_effect = KeyError("FAIL")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_emit.assert_called_with(ANY, "Parquet processing error: 'FAIL'")
    mock_queue.Message().change_visibility.assert_called()


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.safe_mode", MagicMock(return_value=False))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_as_parquet_file")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
def test_it_handles_arrow_exceptions(mock_queue, mock_emit, mock_delete, mock_get):
    # Arrange
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_get.return_value = parquet_file
    mock_delete.side_effect = ArrowException("FAIL")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_emit.assert_called_with(ANY, "Parquet processing error: FAIL")
    mock_queue.Message().change_visibility.assert_called()


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.safe_mode", MagicMock(return_value=False))
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
def test_it_validates_messages_with_missing_keys(mock_queue, mock_emit):
    # Act
    execute("{}", "receipt_handle")
    # Assert
    mock_emit.assert_called()
    mock_queue.Message().change_visibility.assert_called()


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.safe_mode", MagicMock(return_value=False))
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
def test_it_validates_messages_with_invalid_body(mock_queue, mock_emit):
    # Act
    execute("NOT JSON", "receipt_handle")
    # Assert that we can proceed even if the JSON can't be deserialised when trying to emit an event
    mock_emit.assert_not_called()
    mock_queue.Message().change_visibility.assert_called()


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.safe_mode", MagicMock(return_value=False))
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_as_parquet_file")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
def test_it_handles_s3_permission_issues(mock_queue, mock_emit, mock_get):
    mock_get.side_effect = ClientError({}, "GetObject")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    msg = mock_emit.call_args[0][1]
    assert msg.startswith("ClientError:")
    mock_queue.Message().change_visibility.assert_called()


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.safe_mode", MagicMock(return_value=False))
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_as_parquet_file")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
def test_it_handles_io_errors(mock_queue, mock_emit, mock_get):
    # Arrange
    mock_get.side_effect = IOError("an error")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_emit.assert_called_with(ANY, "Unable to retrieve object: an error")
    mock_queue.Message().change_visibility.assert_called()


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.safe_mode", MagicMock(return_value=False))
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_as_parquet_file")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
def test_it_handles_file_too_big(mock_queue, mock_emit, mock_get):
    # Arrange
    mock_get.side_effect = MemoryError("Too big")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_emit.assert_called_with(ANY, "Insufficient memory to work on object: Too big")
    mock_queue.Message().change_visibility.assert_called()


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


@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_info")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_tags")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_acl")
@patch("backend.ecs_tasks.delete_files.delete_files.get_grantees")
def test_it_applies_safe_mode(mock_grantees, mock_acl, mock_tagging, mock_standard, mock_requester):
    mock_client = MagicMock()
    mock_requester.return_value = ({}, {})
    mock_standard.return_value = ({}, {})
    mock_tagging.return_value = ({}, {})
    mock_acl.return_value = ({}, {})
    mock_grantees.return_value = ''
    buf = BytesIO()
    save(mock_client, buf, "bucket", "key", True)
    mock_client.upload_fileobj.assert_called_with(buf, 'test', "results/bucket/key", ExtraArgs={})
    mock_client.put_object_acl.assert_not_called()


@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_info")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_tags")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_acl")
@patch("backend.ecs_tasks.delete_files.delete_files.get_grantees")
def test_it_applies_settings_when_saving(mock_grantees, mock_acl, mock_tagging, mock_standard, mock_requester):
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
    save(mock_client, buf, "bucket", "key", False)
    mock_client.upload_fileobj.assert_called_with(buf, "bucket", "key", ExtraArgs={
        "Expires": "123",
        "Metadata": {},
        "RequestPayer": "requester",
        "Tagging": "a=b",
        "GrantFullControl": "id=abc",
        "GrantRead": "id=123",
    })
    mock_client.put_object_acl.assert_not_called()


@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_info")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_tags")
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_acl")
@patch("backend.ecs_tasks.delete_files.delete_files.get_grantees")
def test_it_restores_write_permissions(mock_grantees, mock_acl, mock_tagging, mock_standard, mock_requester):
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
    save(mock_client, buf, "bucket", "key", False)
    mock_client.upload_fileobj.assert_called_with(buf, "bucket", "key", ExtraArgs={
        "GrantFullControl": "id=abc",
    })
    mock_client.put_object_acl.assert_called_with(
        Bucket="bucket",
        Key="key",
        GrantFullControl="id=abc",
        GrantWrite="id=123"
    )


@patch.dict(os.environ, {'JobTable': 'test'})
@patch("backend.ecs_tasks.delete_files.delete_files.safe_mode", MagicMock(return_value=False))
@patch("backend.ecs_tasks.delete_files.delete_files.validate_message", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.get_object_as_parquet_file")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_matches_from_file")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
def test_it_provides_logs_for_acl_fail(mock_save, mock_emit, mock_delete, mock_get):
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_get.return_value = parquet_file
    mock_save.side_effect = ClientError({}, "PutObjectAcl")
    mock_delete.return_value = pa.BufferOutputStream(), {"DeletedRows": 1}
    execute(message_stub(), "receipt_handle")
    mock_save.assert_called()
    mock_emit.assert_called_with(ANY, "ClientError: An error occurred (Unknown) when calling the PutObjectAcl "
                                      "operation: Unknown. Redacted object uploaded successfully but unable to "
                                      "restore WRITE ACL")


def test_it_passes_through_safe_mode():
    safe_mode.cache_clear()
    table = MagicMock()
    table.get_item.return_value = {"Item": {"SafeMode": True}}
    assert safe_mode(table, "123")
    table.get_item.return_value = {"Item": {"SafeMode": False}}
    assert not safe_mode(table, "456")


def test_it_raises_for_item_not_found():
    safe_mode.cache_clear()
    table = MagicMock()
    table.get_item.return_value = {}
    with pytest.raises(ValueError):
        safe_mode(table, "123")


def message_stub(**kwargs):
    return json.dumps({
        "JobId": "1234",
        "Object": "s3://bucket/path/basic.parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": ["12345", "23456"]}],
        **kwargs
    })
