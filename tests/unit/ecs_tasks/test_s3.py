import datetime
import json

from mock import patch, MagicMock, call, ANY
from io import BytesIO

import pytest
from botocore.exceptions import ClientError

from backend.ecs_tasks.delete_files.s3 import (
    delete_old_versions,
    DeleteOldVersionsError,
    fetch_job_manifest,
    fetch_manifest,
    get_requester_payment,
    get_grantees,
    get_object_acl,
    get_object_info,
    get_object_tags,
    IntegrityCheckFailedError,
    rollback_object_version,
    save,
    s3transfer,
    validate_bucket_versioning,
    verify_object_versions_integrity,
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

    assert e.value.args[0] == "Bucket bucket does not have versioning enabled"


def test_it_throws_when_versioning_suspended():
    validate_bucket_versioning.cache_clear()
    client = MagicMock()
    client.get_bucket_versioning.return_value = {"Status": "Suspended"}

    with pytest.raises(ValueError) as e:
        validate_bucket_versioning(client, "bucket")

    assert e.value.args[0] == "Bucket bucket does not have versioning enabled"


def test_it_throws_when_mfa_delete_enabled():
    validate_bucket_versioning.cache_clear()
    client = MagicMock()
    client.get_bucket_versioning.return_value = {
        "Status": "Enabled",
        "MFADelete": "Enabled",
    }

    with pytest.raises(ValueError) as e:
        validate_bucket_versioning(client, "bucket")

    assert e.value.args[0] == "Bucket bucket has MFA Delete enabled"


def test_it_returns_requester_pays():
    get_requester_payment.cache_clear()
    client = MagicMock()
    client.get_bucket_request_payment.return_value = {"Payer": "Requester"}
    assert (
        {"RequestPayer": "requester"},
        {"Payer": "Requester"},
    ) == get_requester_payment(client, "bucket")


def test_it_returns_empty_for_non_requester_pays():
    get_requester_payment.cache_clear()
    client = MagicMock()
    client.get_bucket_request_payment.return_value = {"Payer": "Owner"}
    assert ({}, {"Payer": "Owner"}) == get_requester_payment(client, "bucket")


@patch("backend.ecs_tasks.delete_files.s3.get_requester_payment")
def test_it_returns_standard_info(mock_requester):
    get_object_info.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    stub = {
        "CacheControl": "cache",
        "ContentDisposition": "content_disposition",
        "ContentEncoding": "content_encoding",
        "ContentLanguage": "content_language",
        "ContentType": "ContentType",
        "Expires": "123",
        "Metadata": {"foo": "bar"},
        "ServerSideEncryption": "see",
        "StorageClass": "STANDARD",
        "SSECustomerAlgorithm": "aws:kms",
        "SSEKMSKeyId": "1234",
        "WebsiteRedirectLocation": "test",
    }
    client.head_object.return_value = stub
    assert stub == get_object_info(client, "bucket", "key")[0]


@patch("backend.ecs_tasks.delete_files.s3.get_requester_payment")
def test_it_strips_empty_standard_info(mock_requester):
    get_object_info.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    stub = {
        "CacheControl": "cache",
        "ContentDisposition": "content_disposition",
        "ContentEncoding": "content_encoding",
        "ContentLanguage": "content_language",
        "ContentType": "ContentType",
        "Expires": "123",
        "Metadata": {"foo": "bar"},
        "ServerSideEncryption": "see",
        "StorageClass": "STANDARD",
        "SSECustomerAlgorithm": "aws:kms",
        "SSEKMSKeyId": "1234",
        "WebsiteRedirectLocation": None,
    }
    client.head_object.return_value = stub
    assert {
        "CacheControl": "cache",
        "ContentDisposition": "content_disposition",
        "ContentEncoding": "content_encoding",
        "ContentLanguage": "content_language",
        "ContentType": "ContentType",
        "Expires": "123",
        "Metadata": {"foo": "bar"},
        "ServerSideEncryption": "see",
        "StorageClass": "STANDARD",
        "SSECustomerAlgorithm": "aws:kms",
        "SSEKMSKeyId": "1234",
    } == get_object_info(client, "bucket", "key")[0]


@patch("backend.ecs_tasks.delete_files.s3.get_requester_payment")
def test_it_handles_versions_for_get_info(mock_requester):
    get_object_info.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    client.head_object.return_value = {}
    get_object_info(client, "bucket", "key")
    client.head_object.assert_called_with(Bucket="bucket", Key="key")
    get_object_info(client, "bucket", "key", "abc123")
    client.head_object.assert_called_with(
        Bucket="bucket", Key="key", VersionId="abc123"
    )


def test_it_gets_tagging_args():
    get_object_tags.cache_clear()
    client = MagicMock()
    client.get_object_tagging.return_value = {
        "TagSet": [{"Key": "a", "Value": "b"}, {"Key": "c", "Value": "d"}]
    }
    assert {
        "Tagging": "a=b&c=d",
    } == get_object_tags(
        client, "bucket", "key"
    )[0]


@patch("backend.ecs_tasks.delete_files.s3.get_requester_payment")
def test_it_handles_versions_for_get_tagging(mock_requester):
    get_object_info.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    client.get_object_tagging.return_value = {"TagSet": []}
    get_object_tags(client, "bucket", "key")
    client.get_object_tagging.assert_called_with(Bucket="bucket", Key="key")
    get_object_tags(client, "bucket", "key", "abc123")
    client.get_object_tagging.assert_called_with(
        Bucket="bucket", Key="key", VersionId="abc123"
    )


@patch("backend.ecs_tasks.delete_files.s3.get_requester_payment")
def test_it_gets_acl_args(mock_requester):
    get_object_acl.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    client.get_object_acl.return_value = {
        "Owner": {"ID": "a"},
        "Grants": [
            {"Grantee": {"ID": "b", "Type": "CanonicalUser"}, "Permission": "READ"},
            {"Grantee": {"ID": "c", "Type": "CanonicalUser"}, "Permission": "READ_ACP"},
        ],
    }
    assert {
        "GrantFullControl": "id=a",
        "GrantRead": "id=b",
        "GrantReadACP": "id=c",
    } == get_object_acl(client, "bucket", "key")[0]


@patch("backend.ecs_tasks.delete_files.s3.get_requester_payment")
def test_it_handles_versions_for_get_acl(mock_requester):
    get_object_info.cache_clear()
    client = MagicMock()
    mock_requester.return_value = {}, {}
    client.get_object_tagging.return_value = {
        "Owner": {"ID": "a"},
        "Grants": [
            {"Grantee": {"ID": "b", "Type": "CanonicalUser"}, "Permission": "READ"},
            {"Grantee": {"ID": "c", "Type": "CanonicalUser"}, "Permission": "READ_ACP"},
        ],
    }
    get_object_acl(client, "bucket", "key")
    client.get_object_acl.assert_called_with(Bucket="bucket", Key="key")
    get_object_acl(client, "bucket", "key", "abc123")
    client.get_object_acl.assert_called_with(
        Bucket="bucket", Key="key", VersionId="abc123"
    )


def test_it_gets_grantees_by_type():
    acl = {
        "Owner": {"ID": "owner_id"},
        "Grants": [
            {
                "Grantee": {"ID": "grantee1", "Type": "CanonicalUser"},
                "Permission": "FULL_CONTROL",
            },
            {
                "Grantee": {"ID": "grantee2", "Type": "CanonicalUser"},
                "Permission": "FULL_CONTROL",
            },
            {
                "Grantee": {
                    "EmailAddress": "grantee3",
                    "Type": "AmazonCustomerByEmail",
                },
                "Permission": "READ",
            },
            {"Grantee": {"URI": "grantee4", "Type": "Group"}, "Permission": "WRITE"},
            {
                "Grantee": {"ID": "grantee5", "Type": "CanonicalUser"},
                "Permission": "READ_ACP",
            },
            {
                "Grantee": {"ID": "grantee6", "Type": "CanonicalUser"},
                "Permission": "WRITE_ACP",
            },
        ],
    }
    assert {"id=grantee1", "id=grantee2"} == get_grantees(acl, "FULL_CONTROL")
    assert {"emailAddress=grantee3"} == get_grantees(acl, "READ")
    assert {"uri=grantee4"} == get_grantees(acl, "WRITE")
    assert {"id=grantee5"} == get_grantees(acl, "READ_ACP")
    assert {"id=grantee6"} == get_grantees(acl, "WRITE_ACP")


@patch("backend.ecs_tasks.delete_files.s3.get_requester_payment")
@patch("backend.ecs_tasks.delete_files.s3.get_object_info")
@patch("backend.ecs_tasks.delete_files.s3.get_object_tags")
@patch("backend.ecs_tasks.delete_files.s3.get_object_acl")
@patch("backend.ecs_tasks.delete_files.s3.get_grantees")
def test_it_applies_settings_when_saving(
    mock_grantees, mock_acl, mock_tagging, mock_standard, mock_requester
):
    mock_client = MagicMock()
    mock_requester.return_value = {"RequestPayer": "requester"}, {"Payer": "Requester"}
    mock_standard.return_value = ({"Expires": "123", "Metadata": {}}, {})
    mock_tagging.return_value = (
        {"Tagging": "a=b"},
        {"TagSet": [{"Key": "a", "Value": "b"}]},
    )
    mock_acl.return_value = (
        {
            "GrantFullControl": "id=abc",
            "GrantRead": "id=123",
        },
        {
            "Owner": {"ID": "owner_id"},
            "Grants": [
                {
                    "Grantee": {"ID": "abc", "Type": "CanonicalUser"},
                    "Permission": "FULL_CONTROL",
                },
                {
                    "Grantee": {"ID": "123", "Type": "CanonicalUser"},
                    "Permission": "READ",
                },
            ],
        },
    )
    mock_grantees.return_value = ""
    buf = BytesIO()
    mock_client.upload_fileobj.return_value = {"VersionId": "abc123"}
    resp = save(mock_client, buf, "bucket", "key", {}, "abc123")
    mock_client.upload_fileobj.assert_called_with(
        buf,
        "bucket",
        "key",
        ExtraArgs={
            "RequestPayer": "requester",
            "Expires": "123",
            "Metadata": {},
            "Tagging": "a=b",
            "GrantFullControl": "id=abc",
            "GrantRead": "id=123",
        },
    )
    assert "abc123" == resp
    mock_client.put_object_acl.assert_not_called()


@patch("backend.ecs_tasks.delete_files.s3.get_requester_payment")
@patch("backend.ecs_tasks.delete_files.s3.get_object_info")
@patch("backend.ecs_tasks.delete_files.s3.get_object_tags")
@patch("backend.ecs_tasks.delete_files.s3.get_object_acl")
@patch("backend.ecs_tasks.delete_files.s3.get_grantees")
def test_it_passes_through_version(
    mock_grantees, mock_acl, mock_tagging, mock_standard, mock_requester
):
    mock_client = MagicMock()
    mock_requester.return_value = {}, {}
    mock_standard.return_value = ({}, {})
    mock_tagging.return_value = ({}, {})
    mock_acl.return_value = ({}, {})
    mock_grantees.return_value = ""
    buf = BytesIO()
    save(mock_client, buf, "bucket", "key", {}, "abc123")
    mock_acl.assert_called_with(mock_client, "bucket", "key", "abc123")
    mock_tagging.assert_called_with(mock_client, "bucket", "key", "abc123")
    mock_standard.assert_called_with(mock_client, "bucket", "key", "abc123")


@patch("backend.ecs_tasks.delete_files.s3.get_requester_payment")
@patch("backend.ecs_tasks.delete_files.s3.get_object_info")
@patch("backend.ecs_tasks.delete_files.s3.get_object_tags")
@patch("backend.ecs_tasks.delete_files.s3.get_object_acl")
@patch("backend.ecs_tasks.delete_files.s3.get_grantees")
def test_it_restores_write_permissions(
    mock_grantees, mock_acl, mock_tagging, mock_standard, mock_requester
):
    mock_client = MagicMock()
    mock_requester.return_value = {}, {}
    mock_standard.return_value = ({}, {})
    mock_tagging.return_value = ({}, {})
    mock_acl.return_value = (
        {
            "GrantFullControl": "id=abc",
        },
        {
            "Owner": {"ID": "owner_id"},
            "Grants": [
                {
                    "Grantee": {"ID": "abc", "Type": "CanonicalUser"},
                    "Permission": "FULL_CONTROL",
                },
                {
                    "Grantee": {"ID": "123", "Type": "CanonicalUser"},
                    "Permission": "WRITE",
                },
            ],
        },
    )
    mock_grantees.return_value = {"id=123"}
    buf = BytesIO()
    mock_client.upload_fileobj.return_value = {"VersionId": "new_version123"}
    save(mock_client, buf, "bucket", "key", "abc123")
    mock_client.put_object_acl.assert_called_with(
        Bucket="bucket",
        Key="key",
        VersionId="new_version123",
        GrantFullControl="id=abc",
        GrantWrite="id=123",
    )


def test_it_verifies_integrity_happy_path():
    s3_mock = MagicMock()
    s3_mock.list_object_versions.return_value = {
        "VersionIdMarker": "v7",
        "Versions": [{"VersionId": "v6", "ETag": "a"}],
    }
    result = verify_object_versions_integrity(
        s3_mock, "bucket", "requirements.txt", "v6", "v7"
    )

    assert result
    s3_mock.list_object_versions.assert_called_with(
        Bucket="bucket",
        Prefix="requirements.txt",
        VersionIdMarker="v7",
        KeyMarker="requirements.txt",
        MaxKeys=1,
    )


def test_it_fails_integrity_when_delete_marker_between():
    s3_mock = MagicMock()
    s3_mock.list_object_versions.return_value = {
        "VersionIdMarker": "v7",
        "Versions": [],
        "DeleteMarkers": [{"VersionId": "v6"}],
    }

    with pytest.raises(IntegrityCheckFailedError) as e:
        result = verify_object_versions_integrity(
            s3_mock, "bucket", "requirements.txt", "v5", "v7"
        )
    assert e.value.args == (
        "A delete marker (v6) was detected for the given object between read and write operations (v5 and v7).",
        s3_mock,
        "bucket",
        "requirements.txt",
        "v7",
    )


def test_it_fails_integrity_when_other_version_between():
    s3_mock = MagicMock()
    s3_mock.list_object_versions.return_value = {
        "VersionIdMarker": "v7",
        "Versions": [{"VersionId": "v6", "ETag": "a"}],
    }

    with pytest.raises(IntegrityCheckFailedError) as e:
        result = verify_object_versions_integrity(
            s3_mock, "bucket", "requirements.txt", "v5", "v7"
        )

    assert e.value.args == (
        "A version (v6) was detected for the given object between read and write operations (v5 and v7).",
        s3_mock,
        "bucket",
        "requirements.txt",
        "v7",
    )


def test_it_fails_integrity_when_no_other_version_before():
    s3_mock = MagicMock()
    s3_mock.list_object_versions.return_value = {
        "VersionIdMarker": "v7",
        "Versions": [],
    }

    with pytest.raises(IntegrityCheckFailedError) as e:
        result = verify_object_versions_integrity(
            s3_mock, "bucket", "requirements.txt", "v5", "v7"
        )

    assert e.value.args == (
        "Previous version (v5) has been deleted.",
        s3_mock,
        "bucket",
        "requirements.txt",
        "v7",
    )


@patch("time.sleep")
def test_it_errors_when_version_to_not_found_after_retries(sleep_mock):
    s3_mock = MagicMock()
    s3_mock.list_object_versions.side_effect = get_list_object_versions_error()

    with pytest.raises(ClientError) as e:
        result = verify_object_versions_integrity(
            s3_mock, "bucket", "requirements.txt", "v7", "v8"
        )

    assert sleep_mock.call_args_list == [call(2), call(4), call(8), call(16), call(32)]
    assert (
        e.value.args[0]
        == "An error occurred (InvalidArgument) when calling the ListObjectVersions operation: Invalid version id specified"
    )


@patch("backend.ecs_tasks.delete_files.s3.paginate")
def test_it_deletes_old_versions(paginate_mock):
    s3_mock = MagicMock()
    paginate_mock.return_value = iter(
        [
            (
                {
                    "VersionId": "v1",
                    "LastModified": datetime.datetime.now()
                    - datetime.timedelta(minutes=4),
                },
                {
                    "VersionId": "d2",
                    "LastModified": datetime.datetime.now()
                    - datetime.timedelta(minutes=3),
                },
            ),
            (
                {
                    "VersionId": "v3",
                    "LastModified": datetime.datetime.now()
                    - datetime.timedelta(minutes=2),
                },
                None,
            ),
        ]
    )

    delete_old_versions(s3_mock, "bucket", "key", "v4")
    paginate_mock.assert_called_with(
        s3_mock,
        s3_mock.list_object_versions,
        ["Versions", "DeleteMarkers"],
        Bucket="bucket",
        Prefix="key",
        VersionIdMarker="v4",
        KeyMarker="key",
    )
    s3_mock.delete_objects.assert_called_with(
        Bucket="bucket",
        Delete={
            "Objects": [
                {"Key": "key", "VersionId": "v1"},
                {"Key": "key", "VersionId": "d2"},
                {"Key": "key", "VersionId": "v3"},
            ],
            "Quiet": True,
        },
    )


@patch("backend.ecs_tasks.delete_files.s3.paginate")
def test_it_handles_high_old_version_count(paginate_mock):
    s3_mock = MagicMock()
    paginate_mock.return_value = iter(
        [
            (
                {
                    "VersionId": "v{}".format(i),
                    "LastModified": datetime.datetime.now()
                    + datetime.timedelta(minutes=i),
                },
                None,
            )
            for i in range(1, 1501)
        ]
    )

    delete_old_versions(s3_mock, "bucket", "key", "v0")
    paginate_mock.assert_called_with(
        s3_mock,
        s3_mock.list_object_versions,
        ["Versions", "DeleteMarkers"],
        Bucket="bucket",
        Prefix="key",
        VersionIdMarker="v0",
        KeyMarker="key",
    )
    assert 2 == s3_mock.delete_objects.call_count
    assert {
        "Bucket": "bucket",
        "Delete": {
            "Objects": [
                {"Key": "key", "VersionId": "v{}".format(i)} for i in range(1, 1001)
            ],
            "Quiet": True,
        },
    } == s3_mock.delete_objects.call_args_list[0][1]
    assert {
        "Bucket": "bucket",
        "Delete": {
            "Objects": [
                {"Key": "key", "VersionId": "v{}".format(i)} for i in range(1001, 1501)
            ],
            "Quiet": True,
        },
    } == s3_mock.delete_objects.call_args_list[1][1]


@patch("backend.ecs_tasks.delete_files.s3.paginate")
def test_it_retries_for_deletion_errors(paginate_mock):
    s3_mock = MagicMock()
    paginate_mock.return_value = iter(
        [
            (
                {
                    "VersionId": "v1",
                    "LastModified": datetime.datetime.now()
                    - datetime.timedelta(minutes=4),
                },
                {
                    "VersionId": "v2",
                    "LastModified": datetime.datetime.now()
                    - datetime.timedelta(minutes=3),
                },
            ),
            (
                {
                    "VersionId": "v3",
                    "LastModified": datetime.datetime.now()
                    - datetime.timedelta(minutes=2),
                },
                None,
            ),
        ]
    )
    s3_mock.delete_objects.side_effect = [
        {
            "Errors": [
                {"VersionId": "v1", "Key": "key", "Message": "InternalServerError"}
            ]
        },
        {
            "Errors": [],
        },
    ]

    delete_old_versions(s3_mock, "bucket", "key", "v4")

    assert s3_mock.delete_objects.call_count == 2


@patch("backend.ecs_tasks.delete_files.s3.paginate")
@patch("backend.ecs_tasks.delete_files.s3.delete_s3_objects")
def test_it_raises_for_deletion_errors(delete_s3_objects_mock, paginate_mock):
    s3_mock = MagicMock()
    paginate_mock.return_value = iter(
        [
            (
                {
                    "VersionId": "v1",
                    "LastModified": datetime.datetime.now()
                    - datetime.timedelta(minutes=4),
                },
                {
                    "VersionId": "v2",
                    "LastModified": datetime.datetime.now()
                    - datetime.timedelta(minutes=3),
                },
            ),
            (
                {
                    "VersionId": "v3",
                    "LastModified": datetime.datetime.now()
                    - datetime.timedelta(minutes=2),
                },
                None,
            ),
        ]
    )
    delete_s3_objects_mock.return_value = {
        "Errors": [{"VersionId": "v1", "Key": "key", "Message": "Version not found"}]
    }
    with pytest.raises(DeleteOldVersionsError):
        delete_old_versions(s3_mock, "bucket", "key", "v4")


@patch("backend.ecs_tasks.delete_files.s3.paginate")
def test_it_handles_client_errors_as_deletion_errors(paginate_mock):
    s3_mock = MagicMock()
    paginate_mock.side_effect = get_list_object_versions_error()
    with pytest.raises(DeleteOldVersionsError):
        delete_old_versions(s3_mock, "bucket", "key", "v3")


def test_it_deletes_new_version_during_rollback():
    s3_mock = MagicMock()
    s3_mock.delete_object.return_value = "result"
    mock_callback = MagicMock()
    result = rollback_object_version(
        s3_mock, "bucket", "requirements.txt", "version23", on_error=mock_callback
    )
    assert result == "result"
    s3_mock.delete_object.assert_called_with(
        Bucket="bucket", Key="requirements.txt", VersionId="version23"
    )
    mock_callback.assert_not_called()


def test_it_handles_error_for_client_error():
    s3_mock = MagicMock()
    s3_mock.delete_object.side_effect = ClientError({}, "DeleteObject")
    mock_callback = MagicMock()
    result = rollback_object_version(
        s3_mock, "bucket", "requirements.txt", "version23", on_error=mock_callback
    )
    mock_callback.assert_called_with(
        "ClientError: An error occurred (Unknown) when calling the DeleteObject "
        "operation: Unknown. Version rollback caused by version integrity conflict "
        "failed"
    )


def test_it_handles_error_for_generic_errors():
    s3_mock = MagicMock()
    s3_mock.delete_object.side_effect = RuntimeError("Some issue")
    mock_callback = MagicMock()
    result = rollback_object_version(
        s3_mock, "bucket", "requirements.txt", "version23", on_error=mock_callback
    )
    mock_callback.assert_called_with(
        "Unknown error: Some issue. Version rollback caused by version integrity "
        "conflict failed"
    )


@patch("backend.ecs_tasks.delete_files.s3.fetch_job_manifest")
def test_it_caches_manifests(mock_fetch):
    fetch_manifest.cache_clear()
    fetch_manifest("s3://path/to/manifest1.json")
    fetch_manifest("s3://path/to/manifest1.json")
    fetch_manifest("s3://path/to/manifest2.json")

    assert mock_fetch.call_count == 2
    mock_fetch.assert_has_calls(
        [call("s3://path/to/manifest1.json"), call("s3://path/to/manifest2.json")],
        any_order=True,
    )


def test_s3transfer_locked_version():
    """
    https://github.com/boto/s3transfer/issues/82#issuecomment-837971614

    We have a monkey patch in place to allow us using boto3's upload_fileobj
    when we write back to S3. The issue is that while the method offers a nice
    wrapper around file operations, such as implementing multipart upload only
    when needed, we don't get the VersionId back as we would by using
    put_object. This monkey patch is in place while we wait some pull requests
    to be merged. In the meanwhile, here is a test that allow us to notice
    any change on the version we use on s3transfer, in order to add extra
    protection against automated library upgrade PRs that may silently introduce
    issues.
    """
    assert s3transfer.__version__ == "0.6.0"


def test_s3transfer_put_object_monkeypatch_returns_response():
    put_object_task = s3transfer.upload.PutObjectTask(MagicMock())
    client_mock = MagicMock()
    client_mock.put_object.return_value = "result"
    file_mock = MagicMock()
    file_mock.__enter__.return_value = b"123"

    resp = put_object_task._main(client_mock, file_mock, "bucket", "key", {})

    client_mock.put_object.assert_called_with(Bucket="bucket", Key="key", Body=b"123")
    assert resp == "result"


def test_s3transfer_complete_multipart_monkeypatch_returns_response():
    cmpu_task = s3transfer.upload.CompleteMultipartUploadTask(MagicMock())
    client_mock = MagicMock()
    client_mock.complete_multipart_upload.return_value = "cmpu_result"

    resp = cmpu_task._main(client_mock, "bucket", "key", "id", [{}], {})

    client_mock.complete_multipart_upload.assert_called_with(
        Bucket="bucket", Key="key", UploadId="id", MultipartUpload={"Parts": [{}]}
    )
    assert resp == "cmpu_result"
