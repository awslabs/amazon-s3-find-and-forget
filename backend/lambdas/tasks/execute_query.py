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


def make_query(query_data):
    """
    Returns a query which will look like
    SELECT DISTINCT "$path" FROM (
        SELECT t."$path"
        FROM "db"."table" t,
            "manifests_db"."manifests_table" m
        WHERE
            m."jobid"='job1234' AND
            m."datamapperid"='dm123' AND
            cast(t."customer_id" as varchar)=m."queryablematchid" AND
                m."queryablecolumns"='customer_id'
            AND partition_key = value

        UNION ALL

        SELECT t."$path"
        FROM "db"."table" t,
            "manifests_db"."manifests_table" m
        WHERE
            m."jobid"='job1234' AND
            m."datamapperid"='dm123' AND
            cast(t."other_customer_id" as varchar)=m."queryablematchid" AND
                m."queryablecolumns"='other_customer_id'
            AND partition_key = value
    )

    Note: 'queryablematchid' and 'queryablecolumns' is a convenience
    stringified value of match_id and its column when the match is simple,
    or a stringified joint value when composite (for instance,
    "John_S3F2COMP_Doe" and "first_name_S3F2COMP_last_name").
    JobId and DataMapperId are both used as partitions for the manifest to
    optimize query execution time.

    :param query_data: a dict which looks like
    {
      "Database":"db",
      "Table": "table",
      "Columns": [
        {"Column": "col", "Type": "Simple"},
        {
          "Columns": ["first_name", "last_name"],
          "Type": "Composite"
        }
      ],
      "PartitionKeys": [{"Key":"k", "Value":"val"}]
    }
    """
    distinct_template = """SELECT DISTINCT "$path" FROM ({column_unions})"""
    single_column_template = """
    SELECT t."$path"
    FROM "{db}"."{table}" t,
        "{manifest_db}"."{manifest_table}" m
    WHERE
        m."jobid"='{job_id}' AND
        m."datamapperid"='{data_mapper_id}' AND
        {queryable_matches}=m."queryablematchid" AND m."queryablecolumns"=\'{queryable_columns}\'
        {partition_filters}
    """
    indent = " " * 4
    cast_as_str = "cast(t.{} as varchar)"
    columns_composite_join_token = ", '{}', ".format(COMPOSITE_JOIN_TOKEN)

    db, table, columns, data_mapper_id, job_id = itemgetter(
        "Database", "Table", "Columns", "DataMapperId", "JobId"
    )(query_data)

    partitions = query_data.get("PartitionKeys", [])
    partition_filters = ""
    for partition in partitions:
        partition_filters += " AND {key} = {value} ".format(
            key=escape_column(partition["Key"]),
            value=escape_item(partition["Value"]),
        )

    column_unions = ""
    for i, col in enumerate(columns):
        if i > 0:
            column_unions += "\n" + indent + "UNION ALL\n"
        is_simple = col["Type"] == "Simple"
        queryable_matches = (
            cast_as_str.format(escape_column(col["Column"]))
            if is_simple
            else cast_as_str.format(escape_column(col["Columns"][0]))
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
        column_unions += single_column_template.format(
            db=db,
            table=table,
            manifest_db=glue_db,
            manifest_table=glue_table,
            job_id=job_id,
            data_mapper_id=data_mapper_id,
            queryable_matches=queryable_matches,
            queryable_columns=queryable_columns,
            partition_filters=partition_filters,
        )
    return distinct_template.format(column_unions=column_unions)


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
