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
}
"""
import os

import boto3

from boto_utils import paginate, batch_sqs_msgs, deserialize_item
from decorators import with_logging

glue_client = boto3.client("glue")
sqs = boto3.resource("sqs")
queue = sqs.Queue(os.getenv("QueryQueue"))


@with_logging
def handler(event, context):
    data_mappers = event["DataMappers"]
    deletion_items = event["DeletionQueue"]
    # For every partition combo of every table, create a query
    for data_mapper in data_mappers:
        queries = []
        db = data_mapper["QueryExecutorParameters"]["Database"]
        table_name = data_mapper["QueryExecutorParameters"]["Table"]
        table = get_table(db, table_name)
        partition_keys = [
            p["Name"] for p in table.get("PartitionKeys", [])
        ]
        columns = [c for c in data_mapper["Columns"]]
        # Handle unpartitioned data
        msg = {
            "DataMapperId": data_mapper["DataMapperId"],
            "Database": db,
            "Table": table_name,
            "Columns": columns,
            "PartitionKeys": [],
            "DeleteOldVersions": data_mapper.get("DeleteOldVersions", False),
        }
        if data_mapper.get("RoleArn", None):
            msg["RoleArn"] = data_mapper["RoleArn"]
        if len(partition_keys) == 0:
            queries.append(msg)
        else:
            partitions = get_partitions(db, table_name)
            for partition in partitions:
                values = partition["Values"]
                queries.append({
                    **msg,
                    "PartitionKeys": [
                        {"Key": partition_keys[i], "Value": v}
                        for i, v in enumerate(values)
                    ],
                })
        # Workout which deletion items should be included in this query
        filtered = []
        for i, query in enumerate(queries):
            applicable_match_ids = [
                item["MatchId"] for item in deletion_items
                if query["DataMapperId"] in item.get("DataMappers", [])
                or len(item.get("DataMappers", [])) == 0
            ]

            # Remove the query if there are no relevant matches
            if len(applicable_match_ids) == 0:
                continue
            else:
                query["Columns"] = [
                    {
                        "Column": c,
                        "MatchIds": [convert_to_col_type(mid, c, table) for mid in applicable_match_ids]
                    } for c in queries[i]["Columns"]
                ]
                filtered.append(query)

        batch_sqs_msgs(queue, filtered)


def get_table(db, table_name):
    return glue_client.get_table(DatabaseName=db, Name=table_name)["Table"]


def get_partitions(db, table_name):
    return list(paginate(glue_client, glue_client.get_partitions, ["Partitions"], **{
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
