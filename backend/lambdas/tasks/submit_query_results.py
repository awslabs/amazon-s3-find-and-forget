"""
Submits results from Athena queries to the Fargate deletion queue
"""
import os

import boto3

from decorators import with_logging
from boto_utils import paginate, batch_sqs_msgs

athena = boto3.client("athena")
sqs = boto3.resource("sqs")
queue = sqs.Queue(os.getenv("QueueUrl"))

MSG_BATCH_SIZE = 500


@with_logging
def handler(event, context):
    query_id = event["QueryId"]
    results = paginate(
        athena, athena.get_query_results, ["ResultSet.Rows"], QueryExecutionId=query_id
    )
    messages = []
    msg_count = 0
    path_field_index = None
    for result in results:
        is_header_row = path_field_index is None
        if is_header_row:
            path_field_index = next(
                (
                    index
                    for (index, d) in enumerate(result["Data"])
                    if d["VarCharValue"] == "$path"
                ),
                None,
            )
        else:
            msg_count += 1
            path = result["Data"][path_field_index]["VarCharValue"]
            msg = {
                "JobId": event["JobId"],
                "Object": path,
                "Columns": event["Columns"],
                "RoleArn": event.get("RoleArn", None),
                "DeleteOldVersions": event.get("DeleteOldVersions", True),
                "IgnoreObjectNotFoundExceptions": event.get(
                    "IgnoreObjectNotFoundExceptions", False
                ),
                "Format": event.get("Format"),
                "Manifest": event.get("Manifest"),
            }
            messages.append({k: v for k, v in msg.items() if v is not None})

        if len(messages) >= MSG_BATCH_SIZE:
            batch_sqs_msgs(queue, messages)
            messages = []

    if len(messages) > 0:
        batch_sqs_msgs(queue, messages)

    return msg_count
