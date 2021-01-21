import json
import os
import uuid
from functools import lru_cache

import boto3
from botocore.exceptions import ClientError

redaction_bucket = os.getenv("RedactionPOCBucket", "RedactionBucket")
s3 = boto3.resource("s3")


@lru_cache()
def get_config():
    try:
        return json.loads(
            s3.Object(redaction_bucket, "config.json")
            .get()
            .get("Body")
            .read()
            .decode("utf-8")
        )["Configuration"]
    except ClientError as e:
        # No Config Set for Redaction POC
        return {}


def log_rows(rows, data_mapper_id):
    log_key = "logs/{}/{}.json".format(data_mapper_id, str(uuid.uuid4()))
    content = ""
    for row in rows:
        content += json.dumps(row) + "\n"
    s3.Bucket(redaction_bucket).put_object(Key=log_key, Body=content)


def find_key(key, obj):
    if not obj:
        return None
    for found_key in obj.keys():
        if key.lower() == found_key.lower():
            return found_key


def pydict_to_array(columns, rows):
    array = []
    num_rows = len(rows[columns[0]])

    for i in range(num_rows):
        item = {}
        for column in columns:
            item[column] = rows[column][i]
        array.append(item)
    return array


def array_to_pydict(columns, array):
    new_rows = {}
    for column in columns:
        new_rows[column] = []
        for item in array:
            new_rows[column].append(item[column])

    return new_rows


def transform_parquet_rows(rows, data_mapper_id):
    return transform_rows(rows, data_mapper_id, "parquet")


def transform_json_rows(rows, data_mapper_id):
    return transform_rows(rows, data_mapper_id, "json")


def transform_rows(rows, data_mapper_id, file_type):
    if len(rows) == 0:
        return rows

    config = get_config()
    data_mapper_config = config.get(
        data_mapper_id,
        {
            "DeletionMode": "FullRow",  # Default: Delete Full Row
            "LogOriginalRows": False,  # Default: Don't log full row
        },
    )

    columns_to_redact = data_mapper_config.get("ColumnsToRedact", [])

    if (
        data_mapper_config.get("DeletionMode", "FullRow") == "FullRow"
        or len(columns_to_redact) == 0
    ):
        return []

    iterable = rows
    if file_type == "parquet":
        columns = list(rows.keys())
        iterable = pydict_to_array(columns, rows)

    if data_mapper_config.get("LogOriginalRows", False):
        log_rows(iterable, data_mapper_id)

    for row in iterable:
        for column_to_redact in columns_to_redact:
            obj = row
            splits = column_to_redact.split(".")
            for i, segment in enumerate(splits):
                current_key = find_key(segment, obj)
                if not current_key:
                    # not found - nothing to do
                    break
                if i == len(splits) - 1:
                    # found it - redact
                    obj[current_key] = columns_to_redact[column_to_redact]
                    break
                else:
                    # keep iterating
                    obj = obj[current_key]
    return iterable if file_type == "json" else array_to_pydict(columns, iterable)
