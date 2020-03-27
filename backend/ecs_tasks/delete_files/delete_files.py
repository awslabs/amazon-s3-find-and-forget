import sys
from collections import Counter
from collections.abc import Iterable
import signal
from functools import lru_cache
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

from boto_utils import emit_event, parse_s3_url

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)

sqs = boto3.resource('sqs', endpoint_url="https://sqs.{}.amazonaws.com".format(os.getenv("AWS_DEFAULT_REGION")))
queue = sqs.Queue(os.getenv("DELETE_OBJECTS_QUEUE"))
s3 = s3fs.S3FileSystem(default_cache_type='none', requester_pays=True, default_fill_cache=False, version_aware=True)


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


def save(client, buf, bucket, key, source_version=None):
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
    metadata_file = os.getenv("ECS_CONTAINER_METADATA_FILE")
    if metadata_file and os.path.isfile(metadata_file):
        with open(metadata_file) as f:
            metadata = json.load(f)
        return "ECSTask_{}".format(metadata.get("TaskARN").rsplit("/", 1)[1])
    else:
        return "ECSTask"


@lru_cache()
def get_bucket_versioning(client, bucket):
    resp = client.get_bucket_versioning(Bucket=bucket)

    return resp['Status'] == "Enabled"


def emit_deletion_event(message_body, stats):
    job_id = message_body["JobId"]
    event_data = {
        "Statistics": stats,
        "Object": message_body["Object"],
    }
    emit_event(job_id, "ObjectUpdated", event_data, get_emitter_id())


def emit_failed_deletion_event(message_body, err_message):
    json_body = json.loads(message_body)
    job_id = json_body.get("JobId")
    if not job_id:
        raise ValueError("Message missing Job ID")
    event_data = {
        "Error": err_message,
        'Message': json_body,
    }
    emit_event(job_id, "ObjectUpdateFailed", event_data, get_emitter_id())


def validate_message(message):
    body = json.loads(message)
    mandatory_keys = ["JobId", "Object", "Columns"]
    for k in mandatory_keys:
        if k not in body:
            raise ValueError("Malformed message. Missing key: %s", k)


def handle_error(sqs_msg, message_body, err_message):
    logger.error(sanitize_message(err_message, message_body))
    try:
        emit_failed_deletion_event(message_body, err_message)
    except KeyError:
        logger.error("Unable to emit failure event due to invalid Job ID")
    except (json.decoder.JSONDecodeError, ValueError):
        logger.error("Unable to emit failure event due to invalid message")
    except ClientError as e:
        logger.error("Unable to emit failure event: %s", str(e))

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
    client = boto3.client("s3")
    msg = queue.Message(receipt_handle)
    try:
        # Parse and validate incoming message
        validate_message(message_body)
        body = json.loads(message_body)
        cols, object_path, job_id = itemgetter('Columns', 'Object', 'JobId')(body)
        input_bucket, input_key = parse_s3_url(object_path)
        if not get_bucket_versioning(client, input_bucket):
            raise ValueError("Bucket {} does not have versioning enabled".format(input_bucket))
        # Download the object in-memory and convert to PyArrow NativeFile
        logger.info("Downloading and opening %s object in-memory", object_path)
        with s3.open(object_path, "rb") as f:
            source_version = f.version_id
            logger.info("Using object version %s as source", source_version)
            infile = load_parquet(f)
            # Write new file in-memory
            logger.info("Generating new parquet file without matches")
            out_sink, stats = delete_matches_from_file(infile, cols)
        if stats["DeletedRows"] > 0:
            with pa.BufferReader(out_sink.getvalue()) as output_buf:
                new_version = save(client, output_buf, input_bucket, input_key, source_version)
                logger.info("New object version: %s", new_version)
        else:
            logger.warning("The object %s was processed successfully but no rows required deletion", object_path)
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
        handle_error(msg, message_body, err_message)
    except ValueError as e:
        err_message = "Unprocessable message: {}".format(str(e))
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


def verify_object_versions_integrity(client, bucket, key, from_version, to_version):

    result = { 'IsValid': True }
    from_index = to_index = -1
    error_template = "A {} ({}) was detected for the given object between read and write operations ({} and {})."

    object_versions = client.list_object_versions(Bucket=bucket, Prefix=key, VersionIdMarker=from_version)
    versions = object_versions.get('Versions', [])
    delete_markers = object_versions.get('DeleteMarkers', [])
    all_versions = sorted(versions + delete_markers, key=lambda x: x['LastModified'])
    
    for i,version in enumerate(all_versions):
        if version['VersionId'] == from_version: from_index = i
        if version['VersionId'] == to_version: to_index = i

    if from_index == -1:
        raise ValueError("version_from ({}) not found".format(from_version))
    if to_index == -1:
        raise ValueError("version_to ({}) not found".format(to_version))
    if to_index < from_index:
        raise ValueError("from_version ({}) is more recent than to_version ({})".format(
            from_version,
            to_version
        ))

    if to_index - from_index != 1:
        result['IsValid'] = False
        conflicting = all_versions[to_index - 1]
        conflicting_version = conflicting['VersionId']
        conflicting_version_type = 'delete marker' if 'ETag' not in conflicting else 'version'
        result['Error'] = error_template.format(
            conflicting_version_type,
            conflicting_version,
            from_version, 
            to_version)

    return result


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
