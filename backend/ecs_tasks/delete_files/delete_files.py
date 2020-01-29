import math
import sys
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

cw_logs = boto3.client("logs")
sqs = boto3.resource('sqs', endpoint_url="https://sqs.{}.amazonaws.com".format(os.getenv("AWS_DEFAULT_REGION")))
queue = sqs.Queue(os.getenv("DELETE_OBJECTS_QUEUE"))
s3 = s3fs.S3FileSystem()


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


def save(s3, new_parquet, destination):
    logger.info("Saving to S3")
    s3.put(new_parquet, destination)
    logger.info("File {} complete".format(destination))


def cleanup(new_parquet):
    os.remove(new_parquet)


def get_container_id():
    metadata_file = os.getenv("ECS_CONTAINER_METADATA_FILE")
    if metadata_file and os.path.isfile(metadata_file):
        with open(metadata_file) as f:
            metadata = json.load(f)
        return metadata.get("ContainerId")
    else:
        return str(uuid4())


def emit_deletion_event(message_body, stats):
    job_id = message_body["JobId"]
    event_data = {
        "Statistics": stats,
        "Object": message_body["Object"],
    }
    emit_event(job_id, "ObjectUpdated", event_data, "Task_{}".format(get_container_id()))


def emit_failed_deletion_event(message_body, err_message):
    job_id = message_body["JobId"]
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


def check_file_size(s3, object_path):
    object_size = s3.size(object_path)
    if get_max_file_size_bytes() < object_size:
        raise IOError("Insufficient disk space available for object {}. Size: {} GB".format(
            object_path, round(object_size / math.pow(1024, 3), 2)))


def execute(message_body, receipt_handle):
    temp_dest = "/tmp/new.parquet"
    msg = queue.Message(receipt_handle)
    try:
        validate_message(message_body)
        stats = {"ProcessedRows": 0, "DeletedRows": 0}
        logger.info("Message received: {0}".format(message_body))
        body = json.loads(message_body)
        logger.info("Opening the file")
        object_path = body["Object"]
        check_file_size(s3, object_path)
        with s3.open(object_path, "rb") as f:
            parquet_file = load_parquet(f, stats)
            schema = parquet_file.metadata.schema.to_arrow_schema().remove_metadata()
            with pq.ParquetWriter(temp_dest, schema, flavor="spark") as writer:
                for i in range(parquet_file.num_row_groups):
                    cols = body["Columns"]
                    delete_and_write(parquet_file, i, cols, writer, stats)
        if stats["DeletedRows"] > 0:
            save(s3, temp_dest, object_path)
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
    except (ValueError, TypeError) as e:
        err_message = "Invalid message received: {}".format(str(e))
        logger.error(err_message)
        msg.change_visibility(VisibilityTimeout=0)
    except (IOError, ClientError) as e:
        err_message = "Unable to retrieve object: {}".format(str(e))
        logger.error(err_message)
        emit_failed_deletion_event(json.loads(message_body), err_message)
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
