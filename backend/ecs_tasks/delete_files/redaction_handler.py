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


def transform_rows(rows, data_mapper_id):
    config = get_config()
    data_mapper_config = config.get(
        data_mapper_id,
        {
            "DeletionMode": "FullRow",  # Default: Delete Full Row
            "LogOriginalRows": False,  # Default: Don't log full row
        },
    )

    if data_mapper_config.get("LogOriginalRows", False):
        log_rows(rows, data_mapper_id)

    columns_to_redact = data_mapper_config.get("ColumnsToRedact", [])

    if (
        data_mapper_config.get("DeletionMode", "FullRow") == "FullRow"
        or len(columns_to_redact) == 0
    ):
        return []

    for row in rows:
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

    return rows
