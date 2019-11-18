"""
Task for generating Athena queries from glue catalogs

Requires a state object as the event which looks like:

{
  "DeletionQueue": [{
    "MatchId": "123",
    "DataMappers": ["mapper_a"]
  }],
  "DataMappers": [
    {
      "Database": "db"
      "Table": "table"
      "Columns" ["a_column"]
    }
  ]
"""
import os

import boto3

from boto_utils import paginate
from decorators import with_logger

client = boto3.client("glue")


@with_logger
def handler(event, context):
    data_mappers = event["DataMappers"]
    deletion_items = event["DeletionQueue"]
    queries = []
    # For every partition combo of every table, create a query
    for data_mapper in data_mappers:
        db = data_mapper["QueryExecutorParameters"]["Database"]
        table_name = data_mapper["QueryExecutorParameters"]["Table"]
        table = get_table(db, table_name)
        partition_keys = [
            p["Name"] for p in table.get("PartitionKeys", [])
        ]
        columns = [c for c in data_mapper["Columns"]]
        # Handle unpartitioned data
        if len(partition_keys) == 0:
            queries.append({
                "DataMapperId": data_mapper["DataMapperId"],
                "Database": db,
                "Table": table_name,
                "Columns": columns,
            })
        else:
            partitions = get_partitions(db, table_name)
            for partition in partitions:
                values = partition["Values"]
                queries.append({
                    "DataMapperId": data_mapper["DataMapperId"],
                    "Database": db,
                    "Table": table_name,
                    "Columns": columns,
                    "PartitionKeys": [
                        {"Key": partition_keys[i], "Value": v}
                        for i, v in enumerate(values)
                    ],
                })

    # Workout which deletion items should be included in this query
    for i, query in enumerate(queries):
        applicable_match_ids = [
            item["MatchId"] for item in deletion_items
            if query["DataMapperId"] in item.get("DataMappers", [])
            or len(item.get("DataMappers", [])) == 0
        ]

        # Remove the query if there are no relevant matches
        if len(applicable_match_ids) == 0:
            del queries[i]
        else:
            queries[i]["Columns"] = [
                {
                    "Column": c,
                    "MatchIds": [convert_to_col_type(mid, c, table) for mid in applicable_match_ids]
                } for c in queries[i]["Columns"]
            ]

    return queries


def get_table(db, table_name):
    return client.get_table(DatabaseName=db, Name=table_name)["Table"]


def get_partitions(db, table_name):
    return list(paginate(client, client.get_partitions, ["Partitions"], **{
        "DatabaseName": db,
        "TableName": table_name
    }))


def convert_to_col_type(val, col, table):
    column = next((i for i in table["StorageDescriptor"]["Columns"] if i["Name"] == col), None)
    if not column:
        raise ValueError("Column {} not found".format(col))

    col_type = column["Type"]

    if col_type == "string" or col_type == "varchar":
        return str(val)
    if col_type == "int" or col_type == "bigint":
        return int(val)

    raise ValueError("Column {} is type {} which is not a supported column type for querying")
