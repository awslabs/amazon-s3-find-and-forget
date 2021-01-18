import os
from operator import itemgetter

import boto3

from decorators import with_logging

client = boto3.client("athena")

COMPOSITE_JOIN_TOKEN = "_S3F2COMP_"

glue_db = os.getenv("GlueDatabase", "s3f2_manifests_database")
glue_table = os.getenv("JobManifestsGlueTable", "s3f2_manifests_table")


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


def make_query(query_data):  # TODO: Update description
    """
    Returns a query which will look like
    SELECT DISTINCT $path
    FROM "db"."table"
    WHERE col1 in (matchid1, matchid2) OR col1 in (matchid1, matchid2) AND partition_key = value"

    :param query_data: a dict which looks like
    {
      "Database":"db",
      "Table": "table",
      "Columns": [
        {"Column": "col", "MatchIds": ["match"], "Type": "Simple"},
        {
          "Columns": ["first_name", "last_name"],
          "MatchIds: [["John", "Doe"]],
          "Type": "Composite"
        }
      ],
      "PartitionKeys": [{"Key":"k", "Value":"val"}]
    }
    """
    template = """
    SELECT DISTINCT t."$path"
    FROM "{db}"."{table}" t,
        "{manifest_db}"."{manifest_table}" m
    WHERE
        m."jobid"='{job_id}' AND
        m."datamapperid"='{data_mapper_id}' AND
        ({column_filters})
    """
    single_column_template = '({}=m."queryablematchid" AND m."queryablecolumns"=\'{}\')'
    multiple_columns_template = "concat({}) in ({})"
    columns_composite_join_token = ", '{}', ".format(COMPOSITE_JOIN_TOKEN)

    db, table, columns, data_mapper_id, job_id = itemgetter(
        "Database", "Table", "Columns", "DataMapperId", "JobId"
    )(query_data)
    partitions = query_data.get("PartitionKeys", [])

    column_filters = ""
    for i, col in enumerate(columns):
        if i > 0:
            column_filters += " OR "
        is_simple = col["Type"] == "Simple"
        queryable_matches = (
            "cast(t.{} as varchar)".format(escape_column(col["Column"]))
            if is_simple
            else "cast(t.{} as varchar)".format(escape_column(col["Columns"][0]))
            if len(col["Columns"]) == 1
            else "concat({})".format(
                columns_composite_join_token.join(
                    "t.{0}".format(escape_column(c)) for c in col["Columns"]
                )
            )
        )
        queryable_columns = (
            col["Column"] if is_simple else COMPOSITE_JOIN_TOKEN.join(col["Columns"])
        )
        column_filters += single_column_template.format(
            queryable_matches, queryable_columns
        )

    for partition in partitions:
        template += " AND {key} = {value} ".format(
            key=escape_column(partition["Key"]), value=escape_item(partition["Value"])
        )
    return template.format(
        db=db,
        table=table,
        manifest_db=glue_db,
        manifest_table=glue_table,
        job_id=job_id,
        data_mapper_id=data_mapper_id,
        column_filters=column_filters,
    )


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
