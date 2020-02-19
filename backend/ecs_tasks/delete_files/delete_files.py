import sys
from collections import Counter
import signal
from functools import lru_cache
from urllib.parse import urlencode, quote_plus
from uuid import uuid4
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

from boto_utils import emit_event

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)

sqs = boto3.resource('sqs', endpoint_url="https://sqs.{}.amazonaws.com".format(os.getenv("AWS_DEFAULT_REGION")))
queue = sqs.Queue(os.getenv("DELETE_OBJECTS_QUEUE"))
safe_mode_bucket = os.getenv("SAFE_MODE_BUCKET")
safe_mode_prefix = os.getenv("SAFE_MODE_PREFIX")
s3 = s3fs.S3FileSystem(default_cache_type='none', requester_pays=True, default_fill_cache=False)


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
        with pq.ParquetWriter(out_stream, schema, flavor="spark") as writer:
            for row_group in range(parquet_file.num_row_groups):
                logger.info("Row group {}/{}".format(str(row_group + 1), parquet_file.num_row_groups))
                df = parquet_file.read_row_group(row_group).to_pandas()
                current_rows = get_row_count(df)
                df = delete_from_dataframe(df, to_delete)
                new_rows = get_row_count(df)
                tab = pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata()
                writer.write_table(tab)
                stats.update({"DeletedRows": current_rows - new_rows})
        return out_stream, stats


def save(client, buf, bucket, key, in_safe_mode=True):
    """
    Save a buffer to S3, preserving any existing properties on the object
    """
    # Get Object Settings
    request_payer_args, _ = get_requester_payment(client, bucket)
    object_info_args, _ = get_object_info(client, bucket, key)
    tagging_args, _ = get_object_tags(client, bucket, key)
    acl_args, acl_resp = get_object_acl(client, bucket, key)
    extra_args = {**request_payer_args, **object_info_args, **tagging_args, **acl_args}
    logger.info("Object settings: {}".format(extra_args))
    # Write Object Back to S3
    output_bucket = bucket if not in_safe_mode else safe_mode_bucket
    output_key = key if not in_safe_mode else "{}{}/{}".format(safe_mode_prefix, bucket, key)
    logger.info("Safe mode is {}. Saving updated object to s3://{}/{}".format(in_safe_mode, output_bucket, output_key))
    client.upload_fileobj(buf, output_bucket, output_key, ExtraArgs=extra_args)
    logger.info("Object uploaded to S3")
    # GrantWrite cannot be set whilst uploading therefore ACLs need to be restored separately
    write_grantees = ",".join(get_grantees(acl_resp, "WRITE"))
    if write_grantees:
        logger.info("WRITE grant found. Restoring additional grantees for object")
        client.put_object_acl(Bucket=output_bucket, Key=output_key, **{
            **request_payer_args,
            **acl_args,
            'GrantWrite': write_grantees,
        })
    logger.info("Processing of file s3://{}/{} complete".format(bucket, key))


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
def get_object_info(client, bucket, key):
    """
    Generates a dict containing the non-ACL/Tagging args supported when uploading to S3.
    HeadObject call will be cached
    :returns tuple containing the info formatted for ExtraArgs and the raw response
    """
    object_info = client.head_object(Bucket=bucket, Key=key, **get_requester_payment(client, bucket)[0])
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
def get_object_tags(client, bucket, key):
    """
    Generates a dict containing the Tagging args supported when uploading to S3
    GetObjectTagging call will be cached
    :returns tuple containing tagging formatted for ExtraArgs and the raw response
    """
    tagging = client.get_object_tagging(Bucket=bucket, Key=key, **get_requester_payment(client, bucket)[0])
    return (_remove_none({
        "Tagging": urlencode({tag["Key"]: tag["Value"] for tag in tagging["TagSet"]}, quote_via=quote_plus)
    }), tagging)


@lru_cache()
def get_object_acl(client, bucket, key):
    """
    Generates a dict containing the ACL args supported when uploading to S3
    GetObjectAcl call will be cached
    :returns tuple containing ACL formatted for ExtraArgs and the raw response
    """
    acl = client.get_object_acl(Bucket=bucket, Key=key, **get_requester_payment(client, bucket)[0])
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
def safe_mode(table, job_id):
    resp = table.get_item(Key={"Id": job_id, "Sk": job_id})
    if not resp.get("Item"):
        raise ValueError("Invalid Job ID")

    return resp["Item"].get("SafeMode", True)


def emit_deletion_event(message_body, stats):
    job_id = message_body["JobId"]
    event_data = {
        "Statistics": stats,
        "Object": message_body["Object"],
    }
    emit_event(job_id, "ObjectUpdated", event_data, get_emitter_id())


def emit_failed_deletion_event(message_body, err_message):
    try:
        json_body = json.loads(message_body)
        job_id = json_body.get("JobId")
        if not job_id:
            raise ValueError("Message missing Job ID")
        event_data = {
            "Error": err_message,
            'Message': json_body,
        }
        emit_event(job_id, "ObjectUpdateFailed", event_data, get_emitter_id())
    except ValueError as e:
        logger.exception("Unable to emit failure event due to invalid message")
    except ClientError as e:
        logger.exception("Unable to emit failure event: {}".format(e))


def validate_message(message):
    body = json.loads(message)
    mandatory_keys = ["JobId", "Object", "Columns"]
    for k in mandatory_keys:
        if k not in body:
            raise ValueError("Malformed message. Missing key: {}".format(k))


def handle_error(sqs_msg, message_body, err_message):
    logger.error(err_message)
    emit_failed_deletion_event(message_body, err_message)
    sqs_msg.change_visibility(VisibilityTimeout=0)


def execute(message_body, receipt_handle):
    logger.info("Message received")
    ddb = boto3.resource("dynamodb")
    table = ddb.Table(os.getenv("JobTable"))
    client = boto3.client("s3")
    msg = queue.Message(receipt_handle)
    try:
        # Parse and validate incoming message
        validate_message(message_body)
        body = json.loads(message_body)
        cols, object_path, job_id = itemgetter('Columns', 'Object', 'JobId')(body)
        in_safe_mode = safe_mode(table, job_id)
        input_bucket, input_key = object_path.replace("s3://", "").split("/", 1)
        # Download the object in-memory and convert to PyArrow NativeFile
        logger.info("Downloading and opening {} object in-memory".format(object_path))
        with s3.open(object_path, "rb") as f:
            infile = load_parquet(f)
            # Write new file in-memory
            logger.info("Generating new parquet file without matches")
            out_sink, stats = delete_matches_from_file(infile, cols)
            if stats["DeletedRows"] > 0:
                with pa.BufferReader(out_sink.getvalue()) as output_buf:
                    save(client, output_buf, input_bucket, input_key, in_safe_mode)
            else:
                logger.warning("The object {} was processed successfully but no rows required deletion".format(object_path))
        msg.delete()
        emit_deletion_event(body, stats)
        return msg
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
    logger.info("Received shutdown signal. Cleaning up {} messages".format(len(msgs)))
    process_pool.terminate()
    for msg in msgs:
        try:
            handle_error(msg, msg.body, "SIGINT/SIGTERM received during processing")
        except (ClientError, ValueError) as e:
            logger.exception("Unable to gracefully cleanup message: {}".format(str(e)))
    sys.exit(1 if len(msgs) > 0 else 0)


if __name__ == '__main__':
    logger.info("CPU count for system: {}".format(cpu_count()))
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
