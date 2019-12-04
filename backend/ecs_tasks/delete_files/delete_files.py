from uuid import uuid4

import boto3
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import time
from boto_utils import log_event


def get_queue():
    sqs_endpoint = "https://sqs.{}.amazonaws.com".format(os.getenv("AWS_DEFAULT_REGION"))
    sqs = boto3.resource(service_name='sqs', endpoint_url=sqs_endpoint)
    return sqs.Queue(os.getenv("DELETE_OBJECTS_QUEUE"))


def load_parquet(f, stats):
    parquet_file = pq.ParquetFile(f, memory_map=False)
    stats["total_rows"] = parquet_file.metadata.num_rows
    return parquet_file


def delete_from_dataframe(df, columns):
    for column in columns:
        df = df[~df[column["Column"]].isin(column["MatchIds"])]
    return pa.Table.from_pandas(
        df, preserve_index=False).replace_schema_metadata()


def delete_and_write(parquet_file, row_group, columns, writer, stats):
    print("Row group {}/{}".format(str(row_group + 1), parquet_file.num_row_groups))
    df = parquet_file.read_row_group(row_group).to_pandas()
    current_rows = len(df.index)
    stats["processed_rows"] += current_rows
    if stats["processed_rows"] > 0:
        print("Processing {} rows ({}/{} {}% completed)...".format(
            current_rows, stats["processed_rows"], stats["total_rows"], int((stats["processed_rows"] * 100) / stats["total_rows"])))
    tab = delete_from_dataframe(df, columns)
    writer.write_table(tab)
    print("wrote table")


def save_and_cleanup(s3, new_parquet, destination):
    print("Saving to S3")
    s3.put(new_parquet, destination)
    os.remove(new_parquet)
    print("File {} complete".format(destination))


def get_container_id():
    metadata_file = os.getenv("ECS_CONTAINER_METADATA_FILE")
    if metadata_file and os.path.isfile(metadata_file):
        with open(metadata_file) as f:
            metadata = json.load(f)
        return metadata.get("ContainerId")
    else:
        return str(uuid4())


def log_deletion(message, stats):
    cw_logs = boto3.client("logs")
    job_id = message["JobId"]
    stream_name = "{}-{}".format(job_id, get_container_id())
    event_data = {
        "Statistics": stats,
        **message,
    }
    log_event(cw_logs, stream_name, "ObjectUpdated", event_data)


def execute(queue, s3):
    print("Fetching messages...")
    temp_dest = "/tmp/new.parquet"
    messages = queue.receive_messages(WaitTimeSeconds=5, MaxNumberOfMessages=1)
    if len(messages) == 0:
        print("No messages. Sleeping")
        time.sleep(30)
    else:
        for message in messages:
            stats = {"processed_rows": 0}
            print("Message received: {0}".format(message.body))
            body = json.loads(message.body)
            print("Opening the file")
            object_path = body["Object"]
            with s3.open(object_path, "rb") as f:
                parquet_file = load_parquet(f, stats)
                schema = parquet_file.metadata.schema.to_arrow_schema().remove_metadata()
                with pq.ParquetWriter(temp_dest, schema, flavor="spark") as writer:
                    for i in range(parquet_file.num_row_groups):
                        cols = body["Columns"]
                        delete_and_write(parquet_file, i, cols, writer, stats)
            save_and_cleanup(s3, temp_dest, object_path)
            message.delete()
            log_deletion(body, stats)


if __name__ == '__main__':
    queue = get_queue()
    s3 = s3fs.S3FileSystem()
    while 1:
        execute(queue, s3)
