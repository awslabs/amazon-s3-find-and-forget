import os
import json
import boto3

from decorators import with_logging

client = boto3.client("athena")


@with_logging
def handler(event, context):
    data = event["QueryData"]
    response = client.start_query_execution(
        QueryString=make_query(data),
        ResultConfiguration={
            "OutputLocation": "s3://{bucket}/{prefix}/".format(
                bucket=event["Bucket"], prefix=event["Prefix"]
            )
        },
        WorkGroup=os.getenv("WorkGroup", "primary"),
    )

    return response["QueryExecutionId"]


def make_query(query_data):
    """
    Returns a query which will look like
    SELECT DISTINCT $path
    FROM "db"."table"
    WHERE col1 in (matchid1, matchid2) OR col1 in (matchid1, matchid2) AND partition_key = value"

    :param query_data: a dict which looks like
    {
      "Database":"db",
      "Table": "table",
      "Columns": [{"Column": "col, "MatchIds": ["match"]}],
      "PartitionKeys": [{"Key":"k", "Value":"val"}]
    }
    """
    template = """
    SELECT DISTINCT(t."$path")
    FROM "{db}"."{table}" t
    INNER JOIN "{deletion_queue_db}"."{deletion_queue_table}" dq on ({join_part})
    {partitions_part}
    """.strip()

    columns = query_data["Columns"]
    db = query_data["Database"]
    table = query_data["Table"]
    athena_deletion_queue_db = query_data["DeletionQueueDb"]
    athena_deletion_queue_table = query_data["DeletionQueueTableName"]
    partitions = query_data.get("PartitionKeys", [])
    partitions_list = []
    columns_match = []
    for i, col_name in enumerate(columns):
        columns_match.append(' t.{col_name} = dq.{col_name} '.format(col_name=col_name))
    join_part = 'AND'.join(columns_match)

    for partition in partitions:
        partitions_list.append("{key} = {value}"
                               .format(key=escape_column(partition["Key"]), value=escape_item(partition["Value"])))
    partitions_part = "WHERE " + " AND ".join(partitions_list) if len(partitions) > 0 else ''

    return template.format(db=db,
                           table=table,
                           deletion_queue_db=athena_deletion_queue_db,
                           deletion_queue_table=athena_deletion_queue_table,
                           join_part=join_part,
                           partitions_part=partitions_part)


def escape_column(item):
    return '"{}"'.format(item.replace('"', '""').replace(".", '"."'))


def escape_item(item):
    if item is None:
        return "NULL"
    elif isinstance(item, (int, float)):
        return escape_number(item)
    elif isinstance(item, str):
        return escape_string(item)
    else:
        raise ValueError("Unable to process supplied value")


def escape_number(item):
    return item


def escape_string(item):
    return "'{}'".format(item.replace("'", "''"))
