import math
import sys
from functools import lru_cache
from urllib.parse import urlencode, quote_plus
from uuid import uuid4

import boto3
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import time
import logging
from multiprocessing import Pool, cpu_count

from botocore.exceptions import ClientError
from pyarrow.lib import ArrowException

from boto_utils import emit_event

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)

ddb = boto3.resource("dynamodb")
table = ddb.Table(os.getenv("JobTable"))
sqs = boto3.resource('sqs', endpoint_url="https://sqs.{}.amazonaws.com".format(os.getenv("AWS_DEFAULT_REGION")))
queue = sqs.Queue(os.getenv("DELETE_OBJECTS_QUEUE"))
safe_mode_bucket = os.getenv("SAFE_MODE_BUCKET")
safe_mode_prefix = os.getenv("SAFE_MODE_PREFIX")
s3 = s3fs.S3FileSystem()


def _remove_none(d: dict):
    return {k: v for k, v in d.items() if v is not None and v is not ''}


def load_parquet(f, stats):
    parquet_file = pq.ParquetFile(f, memory_map=False)
    stats["TotalRows"] = parquet_file.metadata.num_rows
    return parquet_file


def delete_from_dataframe(df, columns, stats):
    original = len(df)
    for column in columns:
        df = df[~df[column["Column"]].isin(column["MatchIds"])]
    stats["DeletedRows"] += abs(original - len(df))
    return pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata()


def delete_and_write(parquet_file, row_group, columns, writer, stats):
    logger.info("Row group {}/{}".format(str(row_group + 1), parquet_file.num_row_groups))
    df = parquet_file.read_row_group(row_group).to_pandas()
    current_rows = len(df.index)
    stats["ProcessedRows"] += current_rows
    if stats["ProcessedRows"] > 0:
        logger.info("Processing {} rows ({}/{} {}% completed)...".format(
            current_rows, stats["ProcessedRows"], stats["TotalRows"], int((stats["ProcessedRows"] * 100) / stats["TotalRows"])))
    tab = delete_from_dataframe(df, columns, stats)
    writer.write_table(tab)
    logger.info("wrote table")


def save(client, new_parquet, bucket, key):
    # Get Object Settings
    request_payer_args, _ = get_requester_payment(client, bucket)
    object_info_args, _ = get_object_info(client, bucket, key)
    tagging_args, _ = get_object_tags(client, bucket, key)
    acl_args, acl_resp = get_object_acl(client, bucket, key)
    extra_args = {**request_payer_args, **object_info_args, **tagging_args, **acl_args}
    logger.info("Object settings: {}".format(extra_args))
    # Write Object Back to S3
    logger.info("Saving updated object to S3")
    client.upload_file(new_parquet, bucket, key, ExtraArgs=extra_args)
    logger.info("Object uploaded to S3")
    # GrantWrite cannot be set whilst uploading therefore ACLs need to be restored separately
    write_grantees = ",".join(get_grantees(acl_resp, "WRITE"))
    if write_grantees:
        logger.info("WRITE grant found. Restoring additional grantees for object {}: {}".format(key, write_grantees))
        client.put_object_acl(Bucket=bucket, Key=key, **{
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


def cleanup(new_parquet):
    os.remove(new_parquet)


@lru_cache()
def get_container_id():
    metadata_file = os.getenv("ECS_CONTAINER_METADATA_FILE")
    if metadata_file and os.path.isfile(metadata_file):
        with open(metadata_file) as f:
            metadata = json.load(f)
        return metadata.get("ContainerId")
    else:
        return str(uuid4())


@lru_cache()
def safe_mode(job_id):
    return table.get_item(Key={"Id": job_id, "Sk": job_id}).get("SafeMode", True)


def emit_deletion_event(message_body, stats):
    job_id = message_body["JobId"]
    event_data = {
        "Statistics": stats,
        "Object": message_body["Object"],
    }
    emit_event(job_id, "ObjectUpdated", event_data, "Task_{}".format(get_container_id()))


def emit_failed_deletion_event(message_body, err_message):
    job_id = message_body.get("JobId")
    if not job_id:
        raise ValueError("Unable to emit failure event without Job ID")
    event_data = {
        "Error": err_message,
        'Message': message_body,
    }
    emit_event(job_id, "ObjectUpdateFailed", event_data, "Task_{}".format(get_container_id()))


def validate_message(message):
    body = json.loads(message)
    mandatory_keys = ["JobId", "Object", "Columns"]
    for k in mandatory_keys:
        if k not in body:
            raise ValueError("Malformed message. Missing key: {}".format(k))


def get_max_file_size_bytes():
    max_gb = int(os.getenv("MAX_FILE_SIZE_GB", 9))
    return max_gb * math.pow(1024, 3)


def check_object_size(client, bucket, key):
    _, resp = get_object_info(client, bucket, key)
    object_size = resp["ContentLength"]
    if get_max_file_size_bytes() < object_size:
        raise IOError("Insufficient disk space available for object {}. Size: {} GB".format(
            key, round(object_size / math.pow(1024, 3), 2)))


def execute(message_body, receipt_handle):
    logger.info("Message received: {0}".format(message_body))
    client = boto3.client("s3")
    temp_dest = "/tmp/new.parquet"
    msg = queue.Message(receipt_handle)
    try:
        validate_message(message_body)
        stats = {"ProcessedRows": 0, "DeletedRows": 0}
        body = json.loads(message_body)
        job_id = body["JobId"]
        in_safe_mode = safe_mode(job_id)
        object_path = body["Object"]
        input_bucket, input_key = object_path.replace("s3://", "").split("/", 1)
        output_bucket = input_bucket if not in_safe_mode else safe_mode_bucket
        output_key = input_key if not in_safe_mode else safe_mode_prefix + input_key
        check_object_size(client, input_bucket, input_key)
        logger.info("Safe mode is {}. Writing object to s3://{}/{}".format(in_safe_mode, output_bucket, output_key))
        logger.info("Downloading and opening the object {}".format(object_path))
        with s3.open(object_path, "rb") as f:
            parquet_file = load_parquet(f, stats)
            schema = parquet_file.metadata.schema.to_arrow_schema().remove_metadata()
            with pq.ParquetWriter(temp_dest, schema, flavor="spark") as writer:
                for i in range(parquet_file.num_row_groups):
                    cols = body["Columns"]
                    delete_and_write(parquet_file, i, cols, writer, stats)
        if stats["DeletedRows"] > 0:
            save(client, temp_dest, output_bucket, output_key)
        else:
            logger.warning("The object {} was processed successfully but no rows required deletion".format(object_path))
        msg.delete()
        emit_deletion_event(body, stats)
        return object_path
    except (KeyError, ArrowException) as e:
        err_message = "Parquet processing error: {}".format(str(e))
        logger.error(err_message)
        emit_failed_deletion_event(json.loads(message_body), err_message)
        msg.change_visibility(VisibilityTimeout=0)
    except IOError as e:
        err_message = "Unable to retrieve object: {}".format(str(e))
        logger.error(err_message)
        emit_failed_deletion_event(json.loads(message_body), err_message)
        msg.change_visibility(VisibilityTimeout=0)
    except ClientError as e:
        err_message = "ClientError: {}".format(str(e))
        if e.operation_name == "PutObjectAcl":
            err_message += ". Redacted object uploaded successfully but unable to restore WRITE ACL"
        logger.error(err_message)
        emit_failed_deletion_event(json.loads(message_body), err_message)
        msg.change_visibility(VisibilityTimeout=0)
    except Exception as e:
        err_message = "Unprocessable message: {}".format(str(e))
        try:
            json_body = json.loads(message_body)
            emit_failed_deletion_event(json_body, err_message)
        except Exception as e:
            logger.warning("Failed to emit event for message {}: {}".format(message_body, str(e)))
        logger.error(err_message)
        msg.change_visibility(VisibilityTimeout=0)
    finally:
        if os.path.exists(temp_dest):
            cleanup(temp_dest)


if __name__ == '__main__':
    logger.info("CPU count for system: {}".format(cpu_count()))
    pool = Pool()  # Use max available
    while 1:
        logger.info("Fetching messages...")
        messages = queue.receive_messages(WaitTimeSeconds=5, MaxNumberOfMessages=1)
        if len(messages) == 0:
            logger.info("No messages. Sleeping")
            time.sleep(30)
        else:
            for m in messages:
                pool.apply(execute, args=(m.body, m.receipt_handle))
