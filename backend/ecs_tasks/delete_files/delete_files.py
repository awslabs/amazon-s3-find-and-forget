import math
from uuid import uuid4

import boto3
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import time

from botocore.exceptions import ClientError
from pyarrow.lib import ArrowException

from boto_utils import log_event

cw_logs = boto3.client("logs")


def get_queue(queue_url):
    sqs_endpoint = "https://sqs.{}.amazonaws.com".format(os.getenv("AWS_DEFAULT_REGION"))
    sqs = boto3.resource(service_name='sqs', endpoint_url=sqs_endpoint)
    return sqs.Queue(queue_url)


def load_parquet(f, stats):
    parquet_file = pq.ParquetFile(f, memory_map=False)
    stats["TotalRows"] = parquet_file.metadata.num_rows
    return parquet_file


def delete_from_dataframe(df, columns):
    for column in columns:
        df = df[~df[column["Column"]].isin(column["MatchIds"])]
    return pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata()


def delete_and_write(parquet_file, row_group, columns, writer, stats):
    print("Row group {}/{}".format(str(row_group + 1), parquet_file.num_row_groups))
    df = parquet_file.read_row_group(row_group).to_pandas()
    current_rows = len(df.index)
    stats["ProcessedRows"] += current_rows
    if stats["ProcessedRows"] > 0:
        print("Processing {} rows ({}/{} {}% completed)...".format(
            current_rows, stats["ProcessedRows"], stats["TotalRows"], int((stats["ProcessedRows"] * 100) / stats["TotalRows"])))
    tab = delete_from_dataframe(df, columns)
    writer.write_table(tab)
    print("wrote table")


def save(s3, new_parquet, destination):
    print("Saving to S3")
    s3.put(new_parquet, destination)
    print("File {} complete".format(destination))


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


def log_deletion(message_body, stats):
    job_id = message_body["JobId"]
    stream_name = "{}-{}".format(job_id, get_container_id())
    event_data = {
        "Statistics": stats,
        **message_body,
    }
    log_event(cw_logs, stream_name, "ObjectUpdated", event_data)


def log_failed_deletion(message_body, err_message):
    job_id = message_body["JobId"]
    stream_name = "{}-{}".format(job_id, get_container_id())
    event_data = {
        "Error": err_message,
        'Message': message_body,
    }
    log_event(cw_logs, stream_name, "ObjectUpdateFailed", event_data)


def validate_message(message):
    body = json.loads(message.body)
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


def execute(queue, s3, dlq):
    print("Fetching messages...")
    temp_dest = "/tmp/new.parquet"
    messages = queue.receive_messages(WaitTimeSeconds=5, MaxNumberOfMessages=1)
    if len(messages) == 0:
        print("No messages. Sleeping")
        time.sleep(30)
    else:
        for message in messages:
            try:
                validate_message(message)
                stats = {"ProcessedRows": 0}
                print("Message received: {0}".format(message.body))
                body = json.loads(message.body)
                print("Opening the file")
                object_path = body["Object"]
                check_file_size(s3, object_path)
                with s3.open(object_path, "rb") as f:
                    parquet_file = load_parquet(f, stats)
                    schema = parquet_file.metadata.schema.to_arrow_schema().remove_metadata()
                    with pq.ParquetWriter(temp_dest, schema, flavor="spark") as writer:
                        for i in range(parquet_file.num_row_groups):
                            cols = body["Columns"]
                            delete_and_write(parquet_file, i, cols, writer, stats)
                save(s3, temp_dest, object_path)
                log_deletion(body, stats)
            except (KeyError, ArrowException) as e:
                err_message = "Parquet processing error: {}".format(str(e))
                print(err_message)
                log_failed_deletion(json.loads(message.body), err_message)
                dlq.send_message(MessageBody=message.body)
            except (ValueError, TypeError) as e:
                err_message = "Invalid message received: {}".format(str(e))
                print(err_message)
                dlq.send_message(MessageBody=message.body)
            except (IOError, ClientError) as e:
                err_message = "Unable to retrieve object: {}".format(str(e))
                print(err_message)
                log_failed_deletion(json.loads(message.body), err_message)
                dlq.send_message(MessageBody=message.body)
            finally:
                message.delete()
                cleanup(temp_dest)


if __name__ == '__main__':
    queue = get_queue(os.getenv("DELETE_OBJECTS_QUEUE"))
    dlq = get_queue(os.getenv("DLQ"))
    s3 = s3fs.S3FileSystem()
    while 1:
        execute(queue, s3, dlq)
