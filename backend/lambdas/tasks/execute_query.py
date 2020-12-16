import os
from operator import itemgetter

import boto3

from decorators import with_logging

client = boto3.client("athena")

COMPOSITE_JOIN_TOKEN = "_S3F2COMP_"


@with_logging
def handler(event, context):
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
      "Columns": [{"Column": "col", "MatchIds": ["match"]}],
      "CompositeColumns": [
        "Columns": ["first_name", "last_name"],
        "MatchIds: [["John", "Doe"]]
      ],
      "PartitionKeys": [{"Key":"k", "Value":"val"}]
    }
    """
    template = """
    SELECT DISTINCT "$path"
    FROM "{db}"."{table}"
    WHERE
        ({column_filters})
    """
    single_column_template = "{} in ({})"
    multiple_columns_template = "concat({}) in ({})"
    columns_composite_join_token = ", '{}', ".format(COMPOSITE_JOIN_TOKEN)

    db, table, columns, composite_columns = itemgetter(
        "Database", "Table", "Columns", "CompositeColumns"
    )(query_data)
    partitions = query_data.get("PartitionKeys", [])

    column_filters = ""
    for i, col in enumerate(columns):
        if i > 0:
            column_filters += " OR "
        column_filters += single_column_template.format(
            escape_column(col["Column"]),
            ", ".join("{0}".format(escape_item(m)) for m in col["MatchIds"]),
        )
    for i, col in enumerate(composite_columns):
        if i > 0 or len(columns) > 0:
            column_filters += " OR "
        column_template = (
            multiple_columns_template
            if len(col["Columns"]) > 1
            else single_column_template
        )
        column_filters += column_template.format(
            columns_composite_join_token.join(
                "{0}".format(escape_column(c)) for c in col["Columns"]
            ),
            ", ".join(
                "{0}".format(escape_item(COMPOSITE_JOIN_TOKEN.join(str(x) for x in m)))
                for m in col["MatchIds"]
            ),
        )
    for partition in partitions:
        template += " AND {key} = {value} ".format(
            key=escape_column(partition["Key"]), value=escape_item(partition["Value"])
        )
    return template.format(db=db, table=table, column_filters=column_filters)


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
