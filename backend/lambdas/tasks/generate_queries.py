"""
Task for generating Athena queries from glue catalogs
"""
import os
import boto3

from boto_utils import paginate, batch_sqs_msgs, deserialize_item
from decorators import with_logging

ddb = boto3.resource("dynamodb")
ddb_client = boto3.client("dynamodb")
glue_client = boto3.client("glue")
sqs = boto3.resource("sqs")

queue = sqs.Queue(os.getenv("QueryQueue"))
jobs_table = ddb.Table(os.getenv("JobTable", "S3F2_Jobs"))
data_mapper_table_name = os.getenv("DataMapperTable", "S3F2_DataMappers")


@with_logging
def handler(event, context):
    deletion_items = get_deletion_queue(event['ExecutionName'])
    for data_mapper in get_data_mappers():
        query_executor = data_mapper["QueryExecutor"]
        if query_executor == "athena":
            queries = generate_athena_queries(data_mapper, deletion_items)
        else:
            raise NotImplementedError("Unsupported data mapper query executor: '{}'".format(query_executor))

        batch_sqs_msgs(queue, queries)


def generate_athena_queries(data_mapper, deletion_items):
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
        "QueryExecutor": data_mapper["QueryExecutor"],
        "Format": data_mapper["Format"],
        "Database": db,
        "Table": table_name,
        "Columns": columns,
        "PartitionKeys": [],
        "DeleteOldVersions": data_mapper.get("DeleteOldVersions", True),
    }
    if data_mapper.get("RoleArn", None):
        msg["RoleArn"] = data_mapper["RoleArn"]
    if len(partition_keys) == 0:
        queries.append(msg)
    else:
        # For every partition combo of every table, create a query
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
            if query["DataMapperId"] in item.get("DataMappers", []) or len(item.get("DataMappers", [])) == 0
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
    return filtered


def get_deletion_queue(job_id):
    resp = jobs_table.get_item(Key={'Id': job_id, 'Sk': job_id})
    return resp.get('Item').get('DeletionQueueItems')


def get_data_mappers():
    results = paginate(ddb_client, ddb_client.scan, "Items", TableName=data_mapper_table_name)
    for result in results:
        yield deserialize_item(result)


def get_table(db, table_name):
    return glue_client.get_table(DatabaseName=db, Name=table_name)["Table"]


def get_partitions(db, table_name):
    return paginate(glue_client, glue_client.get_partitions, ["Partitions"], DatabaseName=db, TableName=table_name)


def convert_to_col_type(val, col, table):
    column = next((i for i in table["StorageDescriptor"]["Columns"] if i["Name"] == col), None)
    if not column:
        raise ValueError("Column {} not found".format(col))

    col_type = column["Type"]

    if col_type in ["char", "string", "varchar"]:
        return str(val)
    if col_type in ["bigint", "int", "smallint", "tinyint"]:
        return int(val)
    if col_type in ["double", "float"]:
        return float(val)

    raise ValueError("Column {} is type {} which is not a supported column type for querying")
