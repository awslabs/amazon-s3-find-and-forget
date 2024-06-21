import argparse
import json
import os
import sys
import signal
import time
import logging
from multiprocessing import cpu_count, get_context
from operator import itemgetter

import boto3
import pyarrow as pa
from boto_utils import get_session, json_lines_iterator, parse_s3_url
from botocore.exceptions import ClientError
from pyarrow.lib import ArrowException

from cse import decrypt, encrypt, is_kms_cse_encrypted
from events import (
    sanitize_message,
    emit_failure_event,
    emit_deletion_event,
    emit_skipped_event,
)
from json_handler import delete_matches_from_json_file
from parquet_handler import delete_matches_from_parquet_file
from s3 import (
    delete_old_versions,
    DeleteOldVersionsError,
    fetch_manifest,
    get_object_info,
    IntegrityCheckFailedError,
    rollback_object_version,
    save,
    validate_bucket_versioning,
    verify_object_versions_integrity,
)

FIVE_MB = 5 * 2**20
ROLE_SESSION_NAME = "s3f2"

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", logging.INFO))
formatter = logging.Formatter("[%(levelname)s] PID:%(process)d> %(message)s")
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)


def handle_error(
    sqs_msg,
    message_body,
    err_message,
    event_name="ObjectUpdateFailed",
    change_msg_visibility=True,
):
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
            sqs_msg.meta.client.exceptions.MessageNotInflight,
            sqs_msg.meta.client.exceptions.ReceiptHandleIsInvalid,
        ) as e:
            logger.error("Unable to change message visibility: %s", str(e))


def handle_skip(sqs_msg, message_body, skip_reason):
    sqs_msg.delete()
    logger.info(sanitize_message(skip_reason, message_body))
    emit_skipped_event(message_body, skip_reason)


def validate_message(message):
    body = json.loads(message)
    mandatory_keys = ["JobId", "Object", "Columns"]
    for k in mandatory_keys:
        if k not in body:
            raise ValueError("Malformed message. Missing key: %s", k)


def delete_matches_from_file(input_file, to_delete, file_format, compressed=False):
    logger.info("Generating new file without matches")
    if file_format == "json":
        return delete_matches_from_json_file(input_file, to_delete, compressed)
    return delete_matches_from_parquet_file(input_file, to_delete)


def build_matches(cols, manifest_object):
    """
    This function takes the columns and the manifests, and returns
    the match_ids grouped by column.
    Input example:
    [{"Column":"customer_id", "Type":"Simple"}]
    Output example:
    [{"Column":"customer_id", "Type":"Simple", "MatchIds":[123, 234]}]
    """
    COMPOSITE_MATCH_TOKEN = "_S3F2COMP_"
    manifest = fetch_manifest(manifest_object)
    matches = {}
    for line in json_lines_iterator(manifest):
        if not line["QueryableColumns"] in matches:
            matches[line["QueryableColumns"]] = []
        is_simple = len(line["Columns"]) == 1
        match = line["MatchId"][0] if is_simple else line["MatchId"]
        matches[line["QueryableColumns"]].append(match)
    return list(
        map(
            lambda c: {
                "MatchIds": matches[
                    (
                        COMPOSITE_MATCH_TOKEN.join(c["Columns"])
                        if "Columns" in c
                        else c["Column"]
                    )
                ],
                **c,
            },
            cols,
        )
    )


def execute(queue_url, message_body, receipt_handle):
    logger.info("Message received")
    queue = get_queue(queue_url)
    msg = queue.Message(receipt_handle)
    try:
        # Parse and validate incoming message
        validate_message(message_body)
        body = json.loads(message_body)
        session = get_session(body.get("RoleArn"), ROLE_SESSION_NAME)
        ignore_not_found_exceptions = body.get("IgnoreObjectNotFoundExceptions", False)
        client = session.client("s3")
        kms_client = session.client("kms")
        cols, object_path, job_id, file_format, manifest_object = itemgetter(
            "Columns", "Object", "JobId", "Format", "Manifest"
        )(body)
        input_bucket, input_key = parse_s3_url(object_path)
        validate_bucket_versioning(client, input_bucket)
        match_ids = build_matches(cols, manifest_object)
        s3 = pa.fs.S3FileSystem(
            region=os.getenv("AWS_DEFAULT_REGION"),
            session_name=ROLE_SESSION_NAME,
            external_id=ROLE_SESSION_NAME,
            role_arn=body.get("RoleArn"),
            load_frequency=60 * 60,
        )
        # Download the object in-memory and convert to PyArrow NativeFile
        logger.info("Downloading and opening %s object in-memory", object_path)
        with s3.open_input_stream(
            "{}/{}".format(input_bucket, input_key),
            buffer_size=FIVE_MB,
        ) as f:
            source_version = f.metadata()["VersionId"].decode("utf-8")
            logger.info("Using object version %s as source", source_version)
            # Write new file in-memory
            compressed = object_path.endswith(".gz")
            object_info, _ = get_object_info(
                client, input_bucket, input_key, source_version
            )
            metadata = object_info["Metadata"]
            is_encrypted = is_kms_cse_encrypted(metadata)
            input_file = decrypt(f, metadata, kms_client) if is_encrypted else f
            out_sink, stats = delete_matches_from_file(
                input_file, match_ids, file_format, compressed
            )
        if stats["DeletedRows"] == 0:
            raise ValueError(
                "The object {} was processed successfully but no rows required deletion".format(
                    object_path
                )
            )
        with pa.BufferReader(out_sink.getvalue()) as output_buf:
            if is_encrypted:
                output_buf, metadata = encrypt(output_buf, metadata, kms_client)
            logger.info("Uploading new object version to S3")
            new_version = save(
                client,
                output_buf,
                input_bucket,
                input_key,
                metadata,
                source_version,
            )
        logger.info("New object version: %s", new_version)
        verify_object_versions_integrity(
            client, input_bucket, input_key, source_version, new_version
        )
        if body.get("DeleteOldVersions"):
            logger.info(
                "Deleting object {} versions older than version {}".format(
                    input_key, new_version
                )
            )
            delete_old_versions(client, input_bucket, input_key, new_version)
        msg.delete()
        emit_deletion_event(body, stats)
    except FileNotFoundError as e:
        err_message = "Apache Arrow S3FileSystem Error: {}".format(str(e))
        if ignore_not_found_exceptions:
            handle_skip(msg, body, "Ignored error: {}".format(err_message))
        else:
            handle_error(msg, message_body, err_message)
    except (KeyError, ArrowException) as e:
        err_message = "Apache Arrow processing error: {}".format(str(e))
        handle_error(msg, message_body, err_message)
    except IOError as e:
        err_message = "Unable to retrieve object: {}".format(str(e))
        handle_error(msg, message_body, err_message)
    except MemoryError as e:
        err_message = "Insufficient memory to work on object: {}".format(str(e))
        handle_error(msg, message_body, err_message)
    except ClientError as e:
        ignore_error = False
        err_message = "ClientError: {}".format(str(e))
        if e.operation_name == "PutObjectAcl":
            err_message += ". Redacted object uploaded successfully but unable to restore WRITE ACL"
        if e.operation_name == "ListObjectVersions":
            err_message += ". Could not verify redacted object version integrity"
        if e.operation_name == "HeadObject" and e.response["Error"]["Code"] == "404":
            ignore_error = ignore_not_found_exceptions
        if ignore_error:
            skip_reason = "Ignored error: {}".format(err_message)
            handle_skip(msg, body, skip_reason)
        else:
            handle_error(msg, message_body, err_message)
    except ValueError as e:
        err_message = "Unprocessable message: {}".format(str(e))
        handle_error(msg, message_body, err_message)
    except DeleteOldVersionsError as e:
        err_message = "Unable to delete previous versions: {}".format(str(e))
        handle_error(msg, message_body, err_message)
    except IntegrityCheckFailedError as e:
        err_description, client, bucket, key, version_id = e.args
        err_message = "Object version integrity check failed: {}".format(
            err_description
        )
        handle_error(msg, message_body, err_message)
        rollback_object_version(
            client,
            bucket,
            key,
            version_id,
            on_error=lambda err: handle_error(
                None, "{}", err, "ObjectRollbackFailed", False
            ),
        )
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


def get_queue(queue_url, **resource_kwargs):
    if not resource_kwargs.get("endpoint_url") and os.getenv("AWS_DEFAULT_REGION"):
        resource_kwargs["endpoint_url"] = "https://sqs.{}.{}".format(
            os.getenv("AWS_DEFAULT_REGION"), os.getenv("AWS_URL_SUFFIX")
        )
    sqs = boto3.resource("sqs", **resource_kwargs)
    return sqs.Queue(queue_url)


def main(queue_url, max_messages, wait_time, sleep_time):
    logger.info("CPU count for system: %s", cpu_count())
    messages = []
    queue = get_queue(queue_url)
    with get_context("spawn").Pool(maxtasksperchild=1) as pool:
        signal.signal(signal.SIGINT, lambda *_: kill_handler(messages, pool))
        signal.signal(signal.SIGTERM, lambda *_: kill_handler(messages, pool))
        while 1:
            logger.info("Fetching messages...")
            messages = queue.receive_messages(
                WaitTimeSeconds=wait_time, MaxNumberOfMessages=max_messages
            )
            if len(messages) == 0:
                logger.info("No messages. Sleeping")
                time.sleep(sleep_time)
            else:
                processes = [(queue_url, m.body, m.receipt_handle) for m in messages]
                pool.starmap(execute, processes)
                messages = []


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="Read and process new deletion tasks from a deletion queue"
    )
    parser.add_argument("--wait_time", type=int, default=5)
    parser.add_argument("--max_messages", type=int, default=1)
    parser.add_argument("--sleep_time", type=int, default=30)
    parser.add_argument(
        "--queue_url", type=str, default=os.getenv("DELETE_OBJECTS_QUEUE")
    )
    return parser.parse_args(args)


if __name__ == "__main__":
    opts = parse_args(sys.argv[1:])
    main(opts.queue_url, opts.max_messages, opts.wait_time, opts.sleep_time)
