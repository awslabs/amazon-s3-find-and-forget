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
    emit_deletion_event, emit_failed_deletion_event, check_object_size, get_max_file_size_bytes, save, get_grantees, \
    get_object_info, get_object_tags, get_object_acl, get_requester_payment

pytestmark = [pytest.mark.unit]


@patch("os.path.exists", MagicMock(return_value=True))
@patch("os.remove")
@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_and_write")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.check_object_size", MagicMock())
def test_happy_path_when_queue_not_empty(mock_save, mock_s3, mock_emit, mock_delete_and_write, mock_load_parquet,
                                         mock_pq_writer, mock_remove):
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
    mock_save.assert_called_with(ANY, tmp_file, "bucket", "path/basic.parquet")
    mock_remove.assert_called_with(tmp_file)


@patch("os.path.exists", MagicMock(return_value=True))
@patch("os.remove")
@patch("backend.ecs_tasks.delete_files.delete_files.logger")
@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_and_write")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.check_object_size", MagicMock())
def test_warning_logged_for_no_deletions(mock_save, mock_s3, mock_emit, mock_delete_and_write, mock_load_parquet,
                                         mock_pq_writer, mock_logger, mock_remove):
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
    mock_save.assert_not_called()
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


@patch("backend.ecs_tasks.delete_files.delete_files.emit_event")
def test_it_throws_if_no_job_id_in_emit(mock_emit):
    with pytest.raises(ValueError):
        emit_failed_deletion_event({}, "Some error")
        mock_emit.assert_not_called()


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
@patch("backend.ecs_tasks.delete_files.delete_files.check_object_size", MagicMock())
def test_it_handles_missing_col_exceptions(mock_queue, mock_emit, mock_load_parquet, mock_delete_write,
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
    mock_emit.assert_called_with(ANY, "Parquet processing error: 'FAIL'")
    mock_queue.Message().change_visibility.assert_called()


@patch("os.path.exists", MagicMock(return_value=True))
@patch("os.remove")
@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_and_write")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.s3", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
@patch("backend.ecs_tasks.delete_files.delete_files.check_object_size", MagicMock())
def test_it_handles_arrow_exceptions(mock_queue, mock_emit, mock_load_parquet, mock_delete_write, mock_remove):
    # Arrange
    mock_delete_write.side_effect = ArrowException("FAIL")
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load_parquet.return_value = parquet_file
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_remove.assert_called()
    mock_emit.assert_called_with(ANY, "Parquet processing error: FAIL")
    mock_queue.Message().change_visibility.assert_called()


@patch("os.path.exists", MagicMock(return_value=False))
@patch("os.remove", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
def test_it_validates_messages_with_missing_keys(mock_queue, mock_emit):
    # Act
    execute("{}", "receipt_handle")
    # Assert
    mock_emit.assert_called()
    mock_queue.Message().change_visibility.assert_called()


@patch("os.path.exists", MagicMock(return_value=False))
@patch("os.remove", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
def test_it_validates_messages_with_invalid_body(mock_queue, mock_emit):
    # Act
    execute("NOT JSON", "receipt_handle")
    # Assert that we can proceed even if the JSON can't be deserialised when trying to emit an event
    mock_emit.assert_not_called()
    mock_queue.Message().change_visibility.assert_called()


@patch("os.path.exists", MagicMock(return_value=False))
@patch("os.remove", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.check_object_size", MagicMock())
def test_it_handles_s3_permission_issues(mock_s3, mock_queue, mock_emit):
    mock_s3.open.side_effect = ClientError({}, "GetObject")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    msg = mock_emit.call_args[0][1]
    assert msg.startswith("ClientError:")
    mock_queue.Message().change_visibility.assert_called()


@patch("os.path.exists", MagicMock(return_value=False))
@patch("os.remove", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.check_object_size")
@patch("backend.ecs_tasks.delete_files.delete_files.queue")
@patch("backend.ecs_tasks.delete_files.delete_files.s3", MagicMock())
def test_it_handles_file_too_big(mock_queue, mock_check_size, mock_emit):
    # Arrange
    mock_check_size.side_effect = IOError("Too big")
    # Act
    execute(message_stub(), "receipt_handle")
    # Assert
    mock_emit.assert_called_with(ANY, "Unable to retrieve object: Too big")
    mock_queue.Message().change_visibility.assert_called()


@patch("backend.ecs_tasks.delete_files.delete_files.get_object_info")
@patch("backend.ecs_tasks.delete_files.delete_files.get_max_file_size_bytes", MagicMock(return_value=9 * math.pow(
    1024, 3)))
def test_it_permits_files_under_max_size(mock_info):
    mock_info.return_value = {}, {"ContentLength": 9 * math.pow(1024, 3)}
    check_object_size(MagicMock(), "bucket", "key")


@patch("backend.ecs_tasks.delete_files.delete_files.get_object_info")
@patch("backend.ecs_tasks.delete_files.delete_files.get_max_file_size_bytes", MagicMock(return_value=9 * math.pow(
    1024, 3)))
def test_it_throws_if_file_too_big(mock_info):
    mock_info.return_value = {}, {"ContentLength": 10 * math.pow(1024, 3)}
    with pytest.raises(IOError):
        check_object_size(MagicMock(), "bucket", "key")


@patch.dict(os.environ, {"MAX_FILE_SIZE_GB": "5"})
def test_it_reads_max_size_from_env():
    resp = get_max_file_size_bytes()
    assert resp == 5 * math.pow(1024, 3)


def test_it_defaults_max_file_size():
    resp = get_max_file_size_bytes()
    assert resp == 9 * math.pow(1024, 3)


def test_it_returns_requester_pays():
    client = MagicMock()
    client.get_bucket_request_payment.return_value = {"Payer": "Requester"}
    assert ({"RequestPayer": "requester"}, {"Payer": "Requester"}) == get_requester_payment(client, "bucket")


def test_it_returns_empty_for_non_requester_pays():
    client = MagicMock()
    client.get_bucket_request_payment.return_value = {"Payer": "Owner"}
    assert ({}, {"Payer": "Owner"}) == get_requester_payment(client, "bucket")


@patch("backend.ecs_tasks.delete_files.delete_files.get_requester_payment")
def test_it_returns_standard_info(mock_requester):
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
    save(mock_client, "filepath", "bucket", "key")
    mock_client.upload_file.assert_called_with("filepath", "bucket", "key", ExtraArgs={
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
    save(mock_client, "filepath", "bucket", "key")
    mock_client.upload_file.assert_called_with("filepath", "bucket", "key", ExtraArgs={
        "GrantFullControl": "id=abc",
    })
    mock_client.put_object_acl.assert_called_with(
        Bucket="bucket",
        Key="key",
        GrantFullControl="id=abc",
        GrantWrite="id=123"
    )


@patch("os.path.exists", MagicMock(return_value=True))
@patch("os.remove")
@patch("backend.ecs_tasks.delete_files.delete_files.pq.ParquetWriter")
@patch("backend.ecs_tasks.delete_files.delete_files.load_parquet")
@patch("backend.ecs_tasks.delete_files.delete_files.delete_and_write")
@patch("backend.ecs_tasks.delete_files.delete_files.emit_failed_deletion_event")
@patch("backend.ecs_tasks.delete_files.delete_files.s3")
@patch("backend.ecs_tasks.delete_files.delete_files.save")
@patch("backend.ecs_tasks.delete_files.delete_files.queue", MagicMock())
@patch("backend.ecs_tasks.delete_files.delete_files.check_object_size", MagicMock())
def test_it_provides_logs_for_acl_fail(mock_save, mock_s3, mock_emit, mock_delete_and_write, mock_load_parquet,
                                       mock_pq_writer, mock_remove):
    parquet_file = MagicMock()
    parquet_file.num_row_groups = 1
    mock_load_parquet.return_value = parquet_file
    mock_save.side_effect = ClientError({}, "PutObjectAcl")

    def dw_side_effect(parquet_file, row_group, columns, writer, stats):
        stats["DeletedRows"] = 1

    mock_delete_and_write.side_effect = dw_side_effect
    execute(message_stub(), "receipt_handle")
    mock_save.assert_called()
    mock_emit.assert_called_with(ANY, "ClientError: An error occurred (Unknown) when calling the PutObjectAcl "
                                      "operation: Unknown. Redacted object uploaded successfully but unable to "
                                      "restore WRITE ACL")


def message_stub(**kwargs):
    return json.dumps({
        "JobId": "1234",
        "Object": "s3://bucket/path/basic.parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": ["12345", "23456"]}],
        **kwargs
    })
