"""
Submits results from Athena queries to the Fargate deletion queue
"""
import os

import boto3

from decorators import with_logger
from boto_utils import paginate, batch_sqs_msgs

athena = boto3.client("athena")
sqs = boto3.resource("sqs")
queue = sqs.Queue(os.getenv("QueueUrl"))


@with_logger
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
    batch_sqs_msgs(queue, [{
        "JobId": event["JobId"],
        "Object": p,
        "Columns": event["Columns"]
    } for p in paths])

    return paths

