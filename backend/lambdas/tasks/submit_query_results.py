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

NUM_OF_MESSAGES_IN_BATCH = 200

@with_logging
def handler(event, context):
    query_id = event["QueryId"]
    results = paginate(
        athena, athena.get_query_results, ["ResultSet.Rows"], QueryExecutionId=query_id
    )
    rows = [result for result in results]
    header_row = rows.pop(0)
    path_field_index = next(
        (
            index
            for (index, d) in enumerate(header_row["Data"])
            if d["VarCharValue"] == "$path"
        ),
        None,
    )

    paths = [row["Data"][path_field_index]["VarCharValue"] for row in rows]
    messages = []
    for p in paths:
        msg = {
            "AllFiles": event["AllFiles"],
            "JobId": event["JobId"],
            "Object": p,
            "QueryBucket": event["Bucket"],
            "QueryKey": event["Key"],
            "RoleArn": event.get("RoleArn", None),
            "DeleteOldVersions": event.get("DeleteOldVersions", True),
            "Format": event.get("Format"),
        }
        messages.append({k: v for k, v in msg.items() if v is not None})
    btached_msgs = [messages[i:i + NUM_OF_MESSAGES_IN_BATCH] for i in range(0, len(messages), NUM_OF_MESSAGES_IN_BATCH)]
    for batch in btached_msgs:
        batch_sqs_msgs(queue, batch)

    return None
