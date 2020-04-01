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


@with_logging
def handler(event, context):
    query_id = event["QueryId"]
    results = paginate(athena, athena.get_query_results, ["ResultSet", "Rows"], **{
        "QueryExecutionId": query_id
    })
    rows = [result for result in results]
    header_row = rows.pop(0)
    path_field_index = next((index for (index, d) in enumerate(header_row["Data"]) if d["VarCharValue"] == "$path"),
                            None)

    paths = [row["Data"][path_field_index]["VarCharValue"] for row in rows]
    messages = []
    for p in paths:
        msg = {
            "JobId": event["JobId"],
            "Object": p,
            "Columns": event["Columns"],
            "RoleArn": event.get("RoleArn", None),
            "DeleteOldVersions": event.get("DeleteOldVersions", False),
        }
        messages.append({k: v for k, v in msg.items() if v is not None})

    batch_sqs_msgs(queue, messages)

    return paths

