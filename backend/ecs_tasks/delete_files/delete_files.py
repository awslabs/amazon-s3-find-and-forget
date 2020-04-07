import sys
from collections import Counter
from collections.abc import Iterable
import signal
from functools import lru_cache
import urllib.request
from urllib.parse import urlencode, quote_plus
from operator import itemgetter

import boto3
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
import time
import logging
from multiprocessing import Pool, cpu_count

import s3fs
from botocore.exceptions import ClientError
from pyarrow.lib import ArrowException

from boto_utils import emit_event, parse_s3_url, get_session, paginate

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", logging.INFO))
formatter = logging.Formatter('[%(levelname)s] %(message)s')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)

sqs = boto3.resource('sqs', endpoint_url="https://sqs.{}.amazonaws.com".format(os.getenv("AWS_DEFAULT_REGION")))
queue = sqs.Queue(os.getenv("DELETE_OBJECTS_QUEUE"))


def _remove_none(d: dict):
    return {k: v for k, v in d.items() if v is not None and v is not ''}


def load_parquet(f):
    return pq.ParquetFile(f, memory_map=False)


def get_row_count(df):
    return len(df.index)


def delete_from_dataframe(df, to_delete):
    for column in to_delete:
        df = df[~df[column["Column"]].isin(column["MatchIds"])]
    return df


def delete_matches_from_file(parquet_file, to_delete):
    """
    Deletes matches from Parquet file where to_delete is a list of dicts where
    each dict contains a column to search and the MatchIds to search for in
    that particular column
    """
    schema = parquet_file.metadata.schema.to_arrow_schema().remove_metadata()
    total_rows = parquet_file.metadata.num_rows
    stats = Counter({"ProcessedRows": total_rows, "DeletedRows": 0})
    with pa.BufferOutputStream() as out_stream:
        with pq.ParquetWriter(out_stream, schema) as writer:
            for row_group in range(parquet_file.num_row_groups):
                logger.info("Row group %s/%s", str(row_group + 1), str(parquet_file.num_row_groups))
                df = parquet_file.read_row_group(row_group).to_pandas()
                current_rows = get_row_count(df)
                df = delete_from_dataframe(df, to_delete)
                new_rows = get_row_count(df)
                tab = pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata()
                writer.write_table(tab)
                stats.update({"DeletedRows": current_rows - new_rows})
        return out_stream, stats


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
        client.put_object_acl(Bucket=bucket, Key=key, VersionId=new_version_id, **{
            **request_payer_args,
            **acl_args,
            'GrantWrite': write_grantees,
        })
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
    return (_remove_none({
        'RequestPayer': "requester" if request_payer["Payer"] == "Requester" else None,
    }), request_payer)


@lru_cache()
def get_object_info(client, bucket, key, version_id=None):
    """
    Generates a dict containing the non-ACL/Tagging args supported when uploading to S3.
    HeadObject call will be cached
    :returns tuple containing the info formatted for ExtraArgs and the raw response
    """
    kwargs = {
        "Bucket": bucket,
        "Key": key,
        **get_requester_payment(client, bucket)[0]
    }
    if version_id:
        kwargs["VersionId"] = version_id
    object_info = client.head_object(**kwargs)
    return (_remove_none({
        'CacheControl': object_info.get("CacheControl"),
        'ContentDisposition': object_info.get("ContentDisposition"),
        'ContentEncoding': object_info.get("ContentEncoding"),
        'ContentLanguage': object_info.get("ContentLanguage"),
        'ContentType': object_info.get("ContentType"),
        'Expires': object_info.get("Expires"),
        'Metadata': object_info.get("Metadata"),
        'ServerSideEncryption': object_info.get("ServerSideEncryption"),
        'StorageClass': object_info.get("StorageClass"),
        'SSECustomerAlgorithm': object_info.get("SSECustomerAlgorithm"),
        'SSEKMSKeyId': object_info.get("SSEKMSKeyId"),
        'WebsiteRedirectLocation': object_info.get("WebsiteRedirectLocation")
    }), object_info)


@lru_cache()
def get_object_tags(client, bucket, key, version_id=None):
    """
    Generates a dict containing the Tagging args supported when uploading to S3
    GetObjectTagging call will be cached
    :returns tuple containing tagging formatted for ExtraArgs and the raw response
    """
    kwargs = {
        "Bucket": bucket,
        "Key": key,
        **get_requester_payment(client, bucket)[0]
    }
    if version_id:
        kwargs["VersionId"] = version_id
    tagging = client.get_object_tagging(**kwargs)
    return (_remove_none({
        "Tagging": urlencode({tag["Key"]: tag["Value"] for tag in tagging["TagSet"]}, quote_via=quote_plus)
    }), tagging)


@lru_cache()
def get_object_acl(client, bucket, key, version_id=None):
    """
    Generates a dict containing the ACL args supported when uploading to S3
    GetObjectAcl call will be cached
    :returns tuple containing ACL formatted for ExtraArgs and the raw response
    """
    kwargs = {
        "Bucket": bucket,
        "Key": key,
        **get_requester_payment(client, bucket)[0]
    }
    if version_id:
        kwargs["VersionId"] = version_id
    acl = client.get_object_acl(**kwargs)
    existing_owner = {"id={}".format(acl["Owner"]["ID"])}
    return (_remove_none({
        'GrantFullControl': ",".join(existing_owner | get_grantees(acl, "FULL_CONTROL")),
        'GrantRead': ",".join(get_grantees(acl, "READ")),
        'GrantReadACP': ",".join(get_grantees(acl, "READ_ACP")),
        'GrantWriteACP': ",".join(get_grantees(acl, "WRITE_ACP")),
    }), acl)


def get_grantees(acl, grant_type):
    prop_map = {
        'CanonicalUser': ('ID', "id"),
        'AmazonCustomerByEmail': ('EmailAddress', "emailAddress"),
        'Group': ('URI', "uri")
    }
    filtered = [grantee["Grantee"] for grantee in acl.get("Grants") if grantee["Permission"] == grant_type]
    grantees = set()
    for grantee in filtered:
        identifier_type = grantee["Type"]
        identifier_prop = prop_map[identifier_type]
        grantees.add("{}={}".format(identifier_prop[1], grantee[identifier_prop[0]]))

    return grantees


@lru_cache()
def get_emitter_id():
    metadata_endpoint = os.getenv("ECS_CONTAINER_METADATA_URI")
    if metadata_endpoint:
        res = ""
        try:
            res = urllib.request.urlopen(metadata_endpoint, timeout=1).read()
            metadata = json.loads(res)
            return "ECSTask_{}".format(metadata["Labels"]["com.amazonaws.ecs.task-arn"].rsplit("/", 1)[1])
        except urllib.error.URLError as e:
            logger.warning("Error when accessing the metadata service: {}".format(e.reason))
        except (AttributeError, KeyError, IndexError) as e:
            logger.warning("Malformed response from the metadata service: {}".format(res))
        except Exception as e:
            logger.warning("Error when getting emitter id from metadata service: {}".format(str(e)))
    
    return "ECSTask"


@lru_cache()
def validate_bucket_versioning(client, bucket):
    resp = client.get_bucket_versioning(Bucket=bucket)
    versioning_enabled = resp.get('Status') == "Enabled"
    mfa_delete_enabled = resp.get('MFADelete') == "Enabled"

    if not versioning_enabled:
        raise ValueError("Bucket {} does not have versioning enabled".format(bucket))

    if mfa_delete_enabled:
        raise ValueError("Bucket {} has MFA Delete enabled".format(bucket))

    return True


def delete_old_versions(client, input_bucket, input_key, new_version):
    try:
        resp = list(paginate(client, client.list_object_versions, ["Versions", "DeleteMarkers"],
                             Bucket=input_bucket, Prefix=input_key, VersionIdMarker=new_version, KeyMarker=input_key))
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
                    'Objects': [
                        {
                            'Key': input_key,
                            'VersionId':  version_id
                        } for version_id in version_ids[i:i+max_deletions]
                    ],
                    'Quiet': True
                }
            )
            errors.extend(resp.get("Errors", []))
        if len(errors) > 0:
            raise DeleteOldVersionsError(errors=[
                "Delete object {} version {} failed: {}".format(e["Key"], e["VersionId"], e["Message"])
                for e in errors
            ])
    except ClientError as e:
        raise DeleteOldVersionsError(errors=[str(e)])


class DeleteOldVersionsError(Exception):
    def __init__(self, errors):
        super().__init__("\n".join(errors))
        self.errors = errors


def emit_deletion_event(message_body, stats):
    job_id = message_body["JobId"]
    event_data = {
        "Statistics": stats,
        "Object": message_body["Object"],
    }
    emit_event(job_id, "ObjectUpdated", event_data, get_emitter_id())


def emit_failure_event(message_body, err_message, event_name):
    json_body = json.loads(message_body)
    job_id = json_body.get("JobId")
    if not job_id:
        raise ValueError("Message missing Job ID")
    event_data = {
        "Error": err_message,
        'Message': json_body,
    }
    emit_event(job_id, event_name, event_data, get_emitter_id())


def validate_message(message):
    body = json.loads(message)
    mandatory_keys = ["JobId", "Object", "Columns"]
    for k in mandatory_keys:
        if k not in body:
            raise ValueError("Malformed message. Missing key: %s", k)


def handle_error(sqs_msg, message_body, err_message, event_name = "ObjectUpdateFailed", change_msg_visibility = True):
    logger.error(sanitize_message(err_message, message_body))
    try:
        emit_failure_event(message_body, err_message, event_name)
    except KeyError:
        logger.error("Unable to emit failure event due to invalid Job ID")
    except (json.decoder.JSONDecodeError, ValueError):
        logger.error("Unable to emit failure event due to invalid message")
    except ClientError as e:
        logger.error("Unable to emit failure event: %s", str(e))

    if change_msg_visibility:
        try:
            sqs_msg.change_visibility(VisibilityTimeout=0)
        except (
            sqs.meta.client.exceptions.MessageNotInflight,
            sqs.meta.client.exceptions.ReceiptHandleIsInvalid,
        ) as e:
            logger.error("Unable to change message visibility: %s", str(e))


def sanitize_message(err_message, message_body):
    """
    Obtain all the known match IDs from the original message and ensure
    they are masked in the given err message
    """
    try:
        sanitised = err_message
        if not isinstance(message_body, dict):
            message_body = json.loads(message_body)
        matches = []
        cols = message_body.get("Columns", [])
        for col in cols:
            match_ids = col.get("MatchIds")
            if isinstance(match_ids, Iterable):
                matches.extend(match_ids)
        for m in matches:
            sanitised = sanitised.replace(m, "*** MATCH ID ***")
        return sanitised
    except (json.decoder.JSONDecodeError, ValueError):
        return err_message


def execute(message_body, receipt_handle):
    logger.info("Message received")
    msg = queue.Message(receipt_handle)
    try:
        # Parse and validate incoming message
        validate_message(message_body)
        body = json.loads(message_body)
        session = get_session(body.get("RoleArn"))
        client = session.client("s3")
        cols, object_path, job_id = itemgetter('Columns', 'Object', 'JobId')(body)
        input_bucket, input_key = parse_s3_url(object_path)
        validate_bucket_versioning(client, input_bucket)
        creds = session.get_credentials().get_frozen_credentials()
        s3 = s3fs.S3FileSystem(
            key=creds.access_key,
            secret=creds.secret_key,
            token=creds.token,
            default_cache_type='none',
            requester_pays=True,
            default_fill_cache=False,
            version_aware=True
        )
        # Download the object in-memory and convert to PyArrow NativeFile
        logger.info("Downloading and opening %s object in-memory", object_path)
        with s3.open(object_path, "rb") as f:
            source_version = f.version_id
            logger.info("Using object version %s as source", source_version)
            infile = load_parquet(f)
            # Write new file in-memory
            logger.info("Generating new parquet file without matches")
            out_sink, stats = delete_matches_from_file(infile, cols)
        if stats["DeletedRows"] == 0:
            raise ValueError("The object {} was processed successfully but no rows required deletion".format(object_path))
        with pa.BufferReader(out_sink.getvalue()) as output_buf:
            new_version = save(s3, client, output_buf, input_bucket, input_key, source_version)
            logger.info("New object version: %s", new_version)
            verify_object_versions_integrity(client, input_bucket, input_key, source_version, new_version)
        if body.get("DeleteOldVersions"):
            logger.info("Deleting object {} versions older than version {}".format(input_key, new_version))
            delete_old_versions(client, input_bucket, input_key, new_version)
        msg.delete()
        emit_deletion_event(body, stats)
    except (KeyError, ArrowException) as e:
        err_message = "Parquet processing error: {}".format(str(e))
        handle_error(msg, message_body, err_message)
    except IOError as e:
        err_message = "Unable to retrieve object: {}".format(str(e))
        handle_error(msg, message_body, err_message)
    except MemoryError as e:
        err_message = "Insufficient memory to work on object: {}".format(str(e))
        handle_error(msg, message_body, err_message)
    except ClientError as e:
        err_message = "ClientError: {}".format(str(e))
        if e.operation_name == "PutObjectAcl":
            err_message += ". Redacted object uploaded successfully but unable to restore WRITE ACL"
        if e.operation_name == "ListObjectVersions":
            err_message += ". Could not verify redacted object version integrity"
        handle_error(msg, message_body, err_message)
    except ValueError as e:
        err_message = "Unprocessable message: {}".format(str(e))
        handle_error(msg, message_body, err_message)
    except IntegrityCheckFailedError as e:
        err_description, client, bucket, key, version_id = e.args
        err_message = "Object version integrity check failed: {}".format(err_description)
        handle_error(msg, message_body, err_message)
        rollback_object_version(client, bucket, key, version_id)
    except DeleteOldVersionsError as e:
        err_message = "Unable to delete previous versions: {}".format(str(e))
        handle_error(msg, message_body, err_message)
    except Exception as e:
        err_message = "Unknown error during message processing: {}".format(str(e))
        handle_error(msg, message_body, err_message)


def kill_handler(msgs, process_pool):
    logger.info("Received shutdown signal. Cleaning up %s messages", str(len(msgs)))
    process_pool.terminate()
    for msg in msgs:
        try:
            handle_error(msg, msg.body, "SIGINT/SIGTERM received during processing")
        except (ClientError, ValueError) as e:
            logger.error("Unable to gracefully cleanup message: %s", str(e))
    sys.exit(1 if len(msgs) > 0 else 0)


def retry_wrapper(fn, retry_wait_seconds = 2, retry_factor = 2, max_retries = 5):
    """ Exponential back-off retry wrapper for ClientError exceptions """

    def wrapper(*args, **kwargs):
        retry_current = 0
        last_error = None

        while retry_current <= max_retries:
            try:
                return fn(*args, **kwargs)
            except ClientError as e:
                nonlocal retry_wait_seconds
                if retry_current == max_retries:
                    break
                last_error = e
                retry_current += 1
                time.sleep(retry_wait_seconds)
                retry_wait_seconds *= retry_factor

        raise last_error

    return wrapper


class IntegrityCheckFailedError(Exception):
    def __init__(self, message, client, bucket, key, version_id):
        self.message = message
        self.client = client
        self.bucket = bucket
        self.key = key
        self.version_id = version_id


def verify_object_versions_integrity(client, bucket, key, from_version_id, to_version_id):
    
    def raise_exception(msg):
        raise IntegrityCheckFailedError(msg, client, bucket, key, to_version_id)

    conflict_error_template = "A {} ({}) was detected for the given object between read and write operations ({} and {})."
    not_found_error_template = "Previous version ({}) has been deleted."

    object_versions = retry_wrapper(client.list_object_versions)(
        Bucket=bucket,
        Prefix=key,
        VersionIdMarker=to_version_id,
        KeyMarker=key,
        MaxKeys=1)

    versions = object_versions.get('Versions', [])
    delete_markers = object_versions.get('DeleteMarkers', [])
    all_versions = versions + delete_markers

    if not len(all_versions):
        return raise_exception(not_found_error_template.format(from_version_id))

    prev_version = all_versions[0]
    prev_version_id = prev_version['VersionId']

    if prev_version_id != from_version_id:
        conflicting_version_type = 'delete marker' if 'ETag' not in prev_version else 'version'
        return raise_exception(conflict_error_template.format(
            conflicting_version_type,
            prev_version_id,
            from_version_id,
            to_version_id))

    return True


def rollback_object_version(client, bucket, key, version):
    """ Delete newly created object version as soon as integrity conflict is detected """
    try:
        return client.delete_object(Bucket=bucket, Key=key, VersionId=version)
    except ClientError as e:
        err_message = "ClientError: {}. Version rollback caused by version integrity conflict failed".format(str(e))
        handle_error(None, "{}", err_message, "ObjectRollbackFailed", False)
    except Exception as e:
        err_message = "Unknown error: {}. Version rollback caused by version integrity conflict failed".format(str(e))
        handle_error(None, "{}", err_message, "ObjectRollbackFailed", False)


if __name__ == '__main__':
    logger.info("CPU count for system: %s", cpu_count())
    messages = []
    with Pool(maxtasksperchild=1) as pool:
        signal.signal(signal.SIGINT, lambda *_: kill_handler(messages, pool))
        signal.signal(signal.SIGTERM, lambda *_: kill_handler(messages, pool))
        while 1:
            logger.info("Fetching messages...")
            messages = queue.receive_messages(WaitTimeSeconds=5, MaxNumberOfMessages=1)
            if len(messages) == 0:
                logger.info("No messages. Sleeping")
                time.sleep(30)
            else:
                processes = [(m.body, m.receipt_handle) for m in messages]
                pool.starmap(execute, processes)
                messages = []
