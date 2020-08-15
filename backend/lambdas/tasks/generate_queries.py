"""
Task for generating Athena queries from glue catalogs
"""
import os
import boto3
import json

from boto_utils import paginate, batch_sqs_msgs, deserialize_item
from decorators import with_logging

ddb = boto3.resource("dynamodb")
ddb_client = boto3.client("dynamodb")
glue_client = boto3.client("glue")
sqs = boto3.resource("sqs")

queue = sqs.Queue(os.getenv("QueryQueue"))
jobs_table = ddb.Table(os.getenv("JobTable", "S3F2_Jobs"))
data_mapper_table_name = os.getenv("DataMapperTable", "S3F2_DataMappers")
s3 = boto3.resource("s3")

NUM_OF_RECORDS_IN_QUERY = 17000


@with_logging
def handler(event, context):
    job_id = event['ExecutionName']
    bucket, deletion_items = get_deletion_queue(job_id)
    for data_mapper in get_data_mappers():
        query_executor = data_mapper["QueryExecutor"]
        if query_executor == "athena":
            queries = generate_athena_queries(data_mapper, deletion_items, bucket, job_id)
        else:
            raise NotImplementedError("Unsupported data mapper query executor: '{}'".format(query_executor))

        batch_sqs_msgs(queue, queries)


def generate_athena_queries(data_mapper, deletion_items, bucket, job_id):
    queries = []
    mapper_deletion_queue_bucket = data_mapper["DeletionQueueBucket"]
    mapper_deletion_queue_key = "{}data.csv".format(data_mapper["DeletionQueuePrefix"])

    db = data_mapper["QueryExecutorParameters"]["Database"]
    table_name = data_mapper["QueryExecutorParameters"]["Table"]
    key = "jobs/{}/query_data/{}/data.json".format(job_id, data_mapper["DataMapperId"])
    table = get_table(db, table_name)
    partition_keys = table.get("PartitionKeys", [])

    columns = [c for c in data_mapper["Columns"]]
    # Handle unpartitioned data
    msg = {
        "DeletionQueueDb": data_mapper["DeletionQueueDb"],
        "DeletionQueueTableName": data_mapper["DeletionQueueTableName"],
        "DataMapperId": data_mapper["DataMapperId"],
        "QueryExecutor": data_mapper["QueryExecutor"],
        "Format": data_mapper["Format"],
        "Database": db,
        "Table": table_name,
        "Columns": columns,
        "QueryBucket": bucket,
        "QueryKey": key,
        "AllFiles": False,  # len(deletion_items) > NUM_OF_RECORDS_IN_QUERY,
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
                    {"Key": partition_keys[i]["Name"], "Value": convert_to_col_type(v, partition_keys[i]["Name"], partition_keys)}
                    for i, v in enumerate(values)
                ],
            })
    # Workout which deletion items should be included in this query
    filtered = []
    applicable_match_ids = [
        item["MatchId"] for item in deletion_items
        if data_mapper["DataMapperId"] in item.get("DataMappers", []) or len(item.get("DataMappers", [])) == 0
    ]

    ### todo:: use or delete
    # payload = {
    #     "Columns": []
    # }
    # for i, c in enumerate(columns):
    #     payload["Columns"].append({
    #             "Column": c,
    #             "MatchIds": [mid.split(",")[i] for mid in applicable_match_ids]
    #         })
    ###
    if len(applicable_match_ids) > 0:
        payload = {
            "Columns": [
                {
                    "Column": c,
                    "MatchIds": [convert_to_col_type(mid, c, table["StorageDescriptor"]["Columns"]) for mid in applicable_match_ids]
                } for c in columns
            ]
        }
        # save data to jobs deletion queue
        obj = s3.Object(bucket, key)
        obj.put(Body=json.dumps(payload))

        # send data to Athena deletion queue
        deletion_queue_object = s3.Object(mapper_deletion_queue_bucket, mapper_deletion_queue_key)
        dq_pl = "{}\n".format(" ,".join(columns)) + "\n".join(applicable_match_ids)
        deletion_queue_object.put(Body=dq_pl)

        filtered = queries
    return filtered


def get_deletion_queue(job_id):
    resp = jobs_table.get_item(Key={'Id': job_id, 'Sk': job_id})
    item = resp.get('Item')
    bucket = item.get('DeletionQueueBucket')
    key = item.get('DeletionQueueKey')
    obj = s3.Object(bucket, key)
    raw_data = obj.get()['Body'].read().decode('utf-8')
    deletion_queue_items = json.loads(raw_data)
    deletion_queue_list = deletion_queue_items["DeletionQueueItems"]
    return bucket, deletion_queue_list


def get_data_mappers():
    results = paginate(ddb_client, ddb_client.scan, "Items", TableName=data_mapper_table_name)
    for result in results:
        yield deserialize_item(result)


def get_table(db, table_name):
    return glue_client.get_table(DatabaseName=db, Name=table_name)["Table"]


def get_partitions(db, table_name):
    return paginate(glue_client, glue_client.get_partitions, ["Partitions"], DatabaseName=db, TableName=table_name)


def convert_to_col_type(val, col, col_descriptor):
    column = next((i for i in col_descriptor if i["Name"] == col), None)
    if not column:
        raise ValueError("Column {} not found".format(col))

    col_type = column["Type"]

    if col_type == "string" or col_type == "varchar":
        return str(val)
    if col_type == "int" or col_type == "bigint":
        return int(val)

    raise ValueError("Column {} is type {} which is not a supported column type for querying")
