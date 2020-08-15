import argparse
import json
import os
import sys
import signal
import time
import logging
from multiprocessing import Pool, cpu_count
from operator import itemgetter

import boto3
import pyarrow as pa
import s3fs
from boto_utils import parse_s3_url, get_session
from botocore.exceptions import ClientError
from pyarrow.lib import ArrowException

from events import sanitize_message, emit_failure_event, emit_deletion_event
from parquet import load_parquet, delete_matches_from_file
from s3 import (
    validate_bucket_versioning,
    save,
    verify_object_versions_integrity,
    delete_old_versions,
    IntegrityCheckFailedError,
    rollback_object_version,
    DeleteOldVersionsError,
    RevertLastVersionError,
    revert_last
)

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", logging.INFO))
formatter = logging.Formatter("[%(levelname)s] %(message)s")
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)
s3_client = boto3.resource("s3")


def handle_error(
    sqs_msg,
    message_body,
    err_message,
    event_name="ObjectUpdateFailed",
    change_msg_visibility=True
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
        print(sqs_msg.meta.client.exceptions.MessageNotInflight)
        try:
            sqs_msg.change_visibility(VisibilityTimeout=0)
        except (
            sqs_msg.meta.client.exceptions.MessageNotInflight,
            sqs_msg.meta.client.exceptions.ReceiptHandleIsInvalid,
        ) as e:
            logger.error("Unable to change message visibility: %s", str(e))


def parse_msg_body_and_revert_last(message_body, client):
    body = json.loads(message_body)
    object_path = body.get("Object")
    input_bucket, input_key = parse_s3_url(object_path)
    revert_last(client, input_bucket, input_key)


def validate_message(message):
    body = json.loads(message)
    mandatory_keys = ["JobId", "Object", "QueryBucket", "QueryKey", "AllFiles"]
    for k in mandatory_keys:
        if k not in body:
            raise ValueError("Malformed message. Missing key: %s", k)


def execute(queue_url, message_body, receipt_handle):
    logger.info("Message received")
    queue = get_queue(queue_url)
    msg = queue.Message(receipt_handle)
    try:
        # Parse and validate incoming message
        validate_message(message_body)
        body = json.loads(message_body)
        session = get_session(body.get("RoleArn"))
        client = session.client("s3")
        query_bucket, query_key, object_path, job_id, all_files = itemgetter("QueryBucket", "QueryKey", "Object", "JobId", "AllFiles")(body)
        obj = s3_client.Object(query_bucket, query_key)
        raw_data = obj.get()['Body'].read().decode('utf-8')
        data = json.loads(raw_data)
        cols = data["Columns"]
        input_bucket, input_key = parse_s3_url(object_path)
        validate_bucket_versioning(client, input_bucket)
        creds = session.get_credentials().get_frozen_credentials()
        s3 = s3fs.S3FileSystem(
            key=creds.access_key,
            secret=creds.secret_key,
            token=creds.token,
            default_cache_type="none",
            requester_pays=True,
            default_fill_cache=False,
            version_aware=True,
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
            if all_files:
                logger.info("The object {} was processed successfully but no rows required deletion".format(object_path))
            else:
                raise ValueError(
                    "The object {} was processed successfully but no rows required deletion".format(
                        object_path
                    )
                )
        else:
            with pa.BufferReader(out_sink.getvalue()) as output_buf:
                new_version = save(
                    s3, client, output_buf, input_bucket, input_key, source_version
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


def purge_queue(queue_url, **resource_kwargs):
    if not resource_kwargs.get("endpoint_url") and os.getenv("AWS_DEFAULT_REGION"):
        resource_kwargs["endpoint_url"] = "https://sqs.{}.amazonaws.com".format(
            os.getenv("AWS_DEFAULT_REGION")
        )
    sqs = boto3.resource("sqs", **resource_kwargs)
    queue = sqs.Queue(queue_url)
    queue.purge()


def get_queue(queue_url, **resource_kwargs):
    if not resource_kwargs.get("endpoint_url") and os.getenv("AWS_DEFAULT_REGION"):
        resource_kwargs["endpoint_url"] = "https://sqs.{}.amazonaws.com".format(
            os.getenv("AWS_DEFAULT_REGION")
        )
    sqs = boto3.resource("sqs", **resource_kwargs)
    return sqs.Queue(queue_url)


def main(queue_url, max_messages, wait_time, sleep_time):
    logger.info("CPU count for system: %s", cpu_count())
    messages = []
    queue = get_queue(queue_url)
    with Pool(maxtasksperchild=1) as pool:
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
