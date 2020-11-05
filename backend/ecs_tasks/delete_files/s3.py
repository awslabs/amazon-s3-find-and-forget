import logging
from functools import lru_cache
from urllib.parse import urlencode, quote_plus

from boto_utils import paginate
from botocore.exceptions import ClientError

from utils import remove_none, retry_wrapper

logger = logging.getLogger(__name__)


def save(s3, client, buf, bucket, key, source_version=None):
    """
    Save a buffer to S3, preserving any existing properties on the object
    """
    # Get Object Settings
    request_payer_args, _ = get_requester_payment(client, bucket)
    object_info_args, _ = get_object_info(client, bucket, key, source_version)
    tagging_args, _ = get_object_tags(client, bucket, key, source_version)
    acl_args, acl_resp = get_object_acl(client, bucket, key, source_version)
    extra_args = {**request_payer_args, **object_info_args, **tagging_args, **acl_args}
    logger.info("Object settings: %s", extra_args)
    # Write Object Back to S3
    logger.info("Saving updated object to s3://%s/%s", bucket, key)
    contents = buf.read()
    with s3.open("s3://{}/{}".format(bucket, key), "wb", **extra_args) as f:
        f.write(contents)
    s3.invalidate_cache()  # TODO: remove once https://github.com/dask/s3fs/issues/294 is resolved
    new_version_id = f.version_id
    logger.info("Object uploaded to S3")
    # GrantWrite cannot be set whilst uploading therefore ACLs need to be restored separately
    write_grantees = ",".join(get_grantees(acl_resp, "WRITE"))
    if write_grantees:
        logger.info("WRITE grant found. Restoring additional grantees for object")
        client.put_object_acl(
            Bucket=bucket,
            Key=key,
            VersionId=new_version_id,
            **{**request_payer_args, **acl_args, "GrantWrite": write_grantees,}
        )
    logger.info("Processing of file s3://%s/%s complete", bucket, key)
    return new_version_id


@lru_cache()
def get_requester_payment(client, bucket):
    """
    Generates a dict containing the request payer args supported when calling S3.
    GetBucketRequestPayment call will be cached
    :returns tuple containing the info formatted for ExtraArgs and the raw response
    """
    request_payer = client.get_bucket_request_payment(Bucket=bucket)
    return (
        remove_none(
            {
                "RequestPayer": "requester"
                if request_payer["Payer"] == "Requester"
                else None,
            }
        ),
        request_payer,
    )


@lru_cache()
def get_object_info(client, bucket, key, version_id=None):
    """
    Generates a dict containing the non-ACL/Tagging args supported when uploading to S3.
    HeadObject call will be cached
    :returns tuple containing the info formatted for ExtraArgs and the raw response
    """
    kwargs = {"Bucket": bucket, "Key": key, **get_requester_payment(client, bucket)[0]}
    if version_id:
        kwargs["VersionId"] = version_id
    object_info = client.head_object(**kwargs)
    return (
        remove_none(
            {
                "CacheControl": object_info.get("CacheControl"),
                "ContentDisposition": object_info.get("ContentDisposition"),
                "ContentEncoding": object_info.get("ContentEncoding"),
                "ContentLanguage": object_info.get("ContentLanguage"),
                "ContentType": object_info.get("ContentType"),
                "Expires": object_info.get("Expires"),
                "Metadata": object_info.get("Metadata"),
                "ServerSideEncryption": object_info.get("ServerSideEncryption"),
                "StorageClass": object_info.get("StorageClass"),
                "SSECustomerAlgorithm": object_info.get("SSECustomerAlgorithm"),
                "SSEKMSKeyId": object_info.get("SSEKMSKeyId"),
                "WebsiteRedirectLocation": object_info.get("WebsiteRedirectLocation"),
            }
        ),
        object_info,
    )


@lru_cache()
def get_object_tags(client, bucket, key, version_id=None):
    """
    Generates a dict containing the Tagging args supported when uploading to S3
    GetObjectTagging call will be cached
    :returns tuple containing tagging formatted for ExtraArgs and the raw response
    """
    kwargs = {"Bucket": bucket, "Key": key}
    if version_id:
        kwargs["VersionId"] = version_id
    tagging = client.get_object_tagging(**kwargs)
    return (
        remove_none(
            {
                "Tagging": urlencode(
                    {tag["Key"]: tag["Value"] for tag in tagging["TagSet"]},
                    quote_via=quote_plus,
                )
            }
        ),
        tagging,
    )


@lru_cache()
def get_object_acl(client, bucket, key, version_id=None):
    """
    Generates a dict containing the ACL args supported when uploading to S3
    GetObjectAcl call will be cached
    :returns tuple containing ACL formatted for ExtraArgs and the raw response
    """
    kwargs = {"Bucket": bucket, "Key": key, **get_requester_payment(client, bucket)[0]}
    if version_id:
        kwargs["VersionId"] = version_id
    acl = client.get_object_acl(**kwargs)
    existing_owner = {"id={}".format(acl["Owner"]["ID"])}
    return (
        remove_none(
            {
                "GrantFullControl": ",".join(
                    existing_owner | get_grantees(acl, "FULL_CONTROL")
                ),
                "GrantRead": ",".join(get_grantees(acl, "READ")),
                "GrantReadACP": ",".join(get_grantees(acl, "READ_ACP")),
                "GrantWriteACP": ",".join(get_grantees(acl, "WRITE_ACP")),
            }
        ),
        acl,
    )


def get_grantees(acl, grant_type):
    """
    Get grant grant

    Args:
        acl: (todo): write your description
        grant_type: (str): write your description
    """
    prop_map = {
        "CanonicalUser": ("ID", "id"),
        "AmazonCustomerByEmail": ("EmailAddress", "emailAddress"),
        "Group": ("URI", "uri"),
    }
    filtered = [
        grantee["Grantee"]
        for grantee in acl.get("Grants")
        if grantee["Permission"] == grant_type
    ]
    grantees = set()
    for grantee in filtered:
        identifier_type = grantee["Type"]
        identifier_prop = prop_map[identifier_type]
        grantees.add("{}={}".format(identifier_prop[1], grantee[identifier_prop[0]]))

    return grantees


@lru_cache()
def validate_bucket_versioning(client, bucket):
    """
    Validate the versioning version.

    Args:
        client: (todo): write your description
        bucket: (str): write your description
    """
    resp = client.get_bucket_versioning(Bucket=bucket)
    versioning_enabled = resp.get("Status") == "Enabled"
    mfa_delete_enabled = resp.get("MFADelete") == "Enabled"

    if not versioning_enabled:
        raise ValueError("Bucket {} does not have versioning enabled".format(bucket))

    if mfa_delete_enabled:
        raise ValueError("Bucket {} has MFA Delete enabled".format(bucket))

    return True


def delete_old_versions(client, input_bucket, input_key, new_version):
    """
    Delete all versions.

    Args:
        client: (todo): write your description
        input_bucket: (todo): write your description
        input_key: (str): write your description
        new_version: (str): write your description
    """
    try:
        resp = list(
            paginate(
                client,
                client.list_object_versions,
                ["Versions", "DeleteMarkers"],
                Bucket=input_bucket,
                Prefix=input_key,
                VersionIdMarker=new_version,
                KeyMarker=input_key,
            )
        )
        versions = [el[0] for el in resp if el[0] is not None]
        delete_markers = [el[1] for el in resp if el[1] is not None]
        versions.extend(delete_markers)
        sorted_versions = sorted(versions, key=lambda x: x["LastModified"])
        version_ids = [v["VersionId"] for v in sorted_versions]
        errors = []
        max_deletions = 1000
        for i in range(0, len(version_ids), max_deletions):
            resp = client.delete_objects(
                Bucket=input_bucket,
                Delete={
                    "Objects": [
                        {"Key": input_key, "VersionId": version_id}
                        for version_id in version_ids[i : i + max_deletions]
                    ],
                    "Quiet": True,
                },
            )
            errors.extend(resp.get("Errors", []))
        if len(errors) > 0:
            raise DeleteOldVersionsError(
                errors=[
                    "Delete object {} version {} failed: {}".format(
                        e["Key"], e["VersionId"], e["Message"]
                    )
                    for e in errors
                ]
            )
    except ClientError as e:
        raise DeleteOldVersionsError(errors=[str(e)])


def verify_object_versions_integrity(
    client, bucket, key, from_version_id, to_version_id
):
    """
    Verify the versions of - versions of a given object.

    Args:
        client: (todo): write your description
        bucket: (str): write your description
        key: (str): write your description
        from_version_id: (int): write your description
        to_version_id: (str): write your description
    """
    def raise_exception(msg):
        """
        Raise an exception with the exception.

        Args:
            msg: (str): write your description
        """
        raise IntegrityCheckFailedError(msg, client, bucket, key, to_version_id)

    conflict_error_template = "A {} ({}) was detected for the given object between read and write operations ({} and {})."
    not_found_error_template = "Previous version ({}) has been deleted."

    object_versions = retry_wrapper(client.list_object_versions)(
        Bucket=bucket,
        Prefix=key,
        VersionIdMarker=to_version_id,
        KeyMarker=key,
        MaxKeys=1,
    )

    versions = object_versions.get("Versions", [])
    delete_markers = object_versions.get("DeleteMarkers", [])
    all_versions = versions + delete_markers

    if not len(all_versions):
        return raise_exception(not_found_error_template.format(from_version_id))

    prev_version = all_versions[0]
    prev_version_id = prev_version["VersionId"]

    if prev_version_id != from_version_id:
        conflicting_version_type = (
            "delete marker" if "ETag" not in prev_version else "version"
        )
        return raise_exception(
            conflict_error_template.format(
                conflicting_version_type,
                prev_version_id,
                from_version_id,
                to_version_id,
            )
        )

    return True


def rollback_object_version(client, bucket, key, version, on_error):
    """ Delete newly created object version as soon as integrity conflict is detected """
    try:
        return client.delete_object(Bucket=bucket, Key=key, VersionId=version)
    except ClientError as e:
        err_message = "ClientError: {}. Version rollback caused by version integrity conflict failed".format(
            str(e)
        )
        on_error(err_message)
    except Exception as e:
        err_message = "Unknown error: {}. Version rollback caused by version integrity conflict failed".format(
            str(e)
        )
        on_error(err_message)


class DeleteOldVersionsError(Exception):
    def __init__(self, errors):
        """
        Initialize errors.

        Args:
            self: (todo): write your description
            errors: (str): write your description
        """
        super().__init__("\n".join(errors))
        self.errors = errors


class IntegrityCheckFailedError(Exception):
    def __init__(self, message, client, bucket, key, version_id):
        """
        Initialize a new bucket.

        Args:
            self: (todo): write your description
            message: (str): write your description
            client: (todo): write your description
            bucket: (str): write your description
            key: (str): write your description
            version_id: (str): write your description
        """
        self.message = message
        self.client = client
        self.bucket = bucket
        self.key = key
        self.version_id = version_id
