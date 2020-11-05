import os

import boto3

from decorators import with_logging

client = boto3.client("athena")


@with_logging
def handler(event, context):
    """
    Main handler.

    Args:
        event: (todo): write your description
        context: (dict): write your description
    """
    response = client.start_query_execution(
        QueryString=make_query(event["QueryData"]),
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
    SELECT DISTINCT "$path"
    FROM "{db}"."{table}"
    WHERE
        ({column_filters})
    """
    db = query_data["Database"]
    table = query_data["Table"]
    columns = query_data["Columns"]
    partitions = query_data.get("PartitionKeys", [])

    column_filters = ""
    for i, col in enumerate(columns):
        if i > 0:
            column_filters = column_filters + " OR "
        column_filters = column_filters + "{} in ({})".format(
            escape_column(col["Column"]),
            ", ".join("{0}".format(escape_item(m)) for m in col["MatchIds"]),
        )
    for partition in partitions:
        template = template + " AND {key} = {value} ".format(
            key=escape_column(partition["Key"]), value=escape_item(partition["Value"])
        )
    return template.format(db=db, table=table, column_filters=column_filters)


def escape_column(item):
    """
    Escape special characters.

    Args:
        item: (str): write your description
    """
    return '"{}"'.format(item.replace('"', '""').replace(".", '"."'))


def escape_item(item):
    """
    Escape the given item.

    Args:
        item: (todo): write your description
    """
    if item is None:
        return "NULL"
    elif isinstance(item, (int, float)):
        return escape_number(item)
    elif isinstance(item, str):
        return escape_string(item)
    else:
        raise ValueError("Unable to process supplied value")


def escape_number(item):
    """
    Return the number of the given item.

    Args:
        item: (todo): write your description
    """
    return item


def escape_string(item):
    """
    Escape special characters in a string.

    Args:
        item: (str): write your description
    """
    return "'{}'".format(item.replace("'", "''"))
