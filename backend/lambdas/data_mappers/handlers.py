"""
DataMapper handlers
"""
import json
import os
import re

import boto3

from boto_utils import DecimalEncoder, get_user_info, running_job_exists
from decorators import (
    with_logging,
    request_validator,
    catch_errors,
    add_cors_headers,
    json_body_loader,
    load_schema,
)

s3f2_flow_bucket = os.getenv("s3f2FlowBucket")
s3f2_temp_bucket = os.getenv("s3f2TempBucket")

dynamodb_resource = boto3.resource("dynamodb")
table = dynamodb_resource.Table(os.getenv("DataMapperTable"))

glue_client = boto3.client("glue")
athena_client = boto3.client("athena")

SUPPORTED_SERDE_LIBS = ["org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"]


@with_logging
@add_cors_headers
@request_validator(load_schema("list_data_mappers"))
@catch_errors
def get_data_mappers_handler(event, context):
    qs = event.get("queryStringParameters")
    if not qs:
        qs = {}
    page_size = int(qs.get("page_size", 10))
    scan_params = {"Limit": page_size}
    start_at = qs.get("start_at")
    if start_at:
        scan_params["ExclusiveStartKey"] = {"DataMapperId": start_at}
    items = table.scan(**scan_params).get("Items", [])
    if len(items) < page_size:
        next_start = None
    else:
        next_start = items[-1]["DataMapperId"]
    return {
        "statusCode": 200,
        "body": json.dumps(
            {"DataMappers": items, "NextStart": next_start}, cls=DecimalEncoder
        ),
    }


@with_logging
@add_cors_headers
@json_body_loader
@request_validator(load_schema("create_data_mapper"))
@catch_errors
def create_data_mapper_handler(event, context):
    path_params = event["pathParameters"]
    body = event["body"]
    validate_mapper(body)
    deletion_db = body["QueryExecutorParameters"]["Database"]
    deletion_table = "deletion_queue_{}".format(camel_to_snake_case(path_params["data_mapper_id"]))
    deletion_queue_prefix = "data_mappers/{}/deletion_queue/".format(path_params["data_mapper_id"])
    generate_athena_table_for_mapper(body, deletion_db, deletion_table, s3f2_flow_bucket, deletion_queue_prefix)
    item = {
        "DataMapperId": path_params["data_mapper_id"],
        "Columns": body["Columns"],
        "QueryExecutor": body["QueryExecutor"],
        "QueryExecutorParameters": body["QueryExecutorParameters"],
        "CreatedBy": get_user_info(event),
        "RoleArn": body["RoleArn"],
        "Format": body.get("Format", "parquet"),
        "DeletionQueueDb": deletion_db,
        "DeletionQueueTableName": deletion_table,
        "DeletionQueueBucket": s3f2_flow_bucket,
        "DeletionQueuePrefix": deletion_queue_prefix,
        "DeleteOldVersions": body.get("DeleteOldVersions", True),
    }
    table.put_item(Item=item)

    return {"statusCode": 201, "body": json.dumps(item)}


@with_logging
@add_cors_headers
@request_validator(load_schema("delete_data_mapper"))
@catch_errors
def delete_data_mapper_handler(event, context):
    if running_job_exists():
        raise ValueError("Cannot delete Data Mappers whilst there is a job in progress")
    data_mapper_id = event["pathParameters"]["data_mapper_id"]
    table.delete_item(Key={"DataMapperId": data_mapper_id})

    return {"statusCode": 204}


def generate_athena_table_for_mapper(mapper, deletion_db, deletion_table_name, deletion_queue_bucket, deletion_queue_key):
    response = athena_client.start_query_execution(
        QueryString=make_query(mapper, deletion_db, deletion_table_name, deletion_queue_bucket, deletion_queue_key),
        ResultConfiguration={
            "OutputLocation": "s3://{bucket}/{prefix}/".format(
                bucket=s3f2_temp_bucket, prefix="data_mappers/queries"
            )
        },
        WorkGroup=os.getenv("WorkGroup", "primary"),
    )


def make_query(mapper, db, table_name, deletion_queue_bucket, deletion_queue_key):
    t = get_table_details_from_mapper(mapper)["Table"]
    columns = t["StorageDescriptor"]["Columns"]
    x = ["`{col_name}` {col_type}".format(col_name=c["Name"], col_type=c["Type"])
         for c in columns if c in mapper["Columns"]]
    deletion_columns = ", ".join(x)

    return """
            CREATE EXTERNAL TABLE {db}.{table_name}(
              {deletion_columns}
              )
            ROW FORMAT SERDE 
              'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
            WITH SERDEPROPERTIES ( 
              'escapeChar'='\\', 
              'quoteChar'='\"', 
              'separatorChar'=',') 
            STORED AS INPUTFORMAT 
              'org.apache.hadoop.mapred.TextInputFormat' 
            OUTPUTFORMAT 
              'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION
              's3://{deletion_queue_bucket}/{deletion_queue_key}/'
            TBLPROPERTIES (
              'has_encrypted_data'='false', 
              'skip.header.line.count'='1', 
              'transient_lastDdlTime'='1596023961')
        """.format(db=db,
                   table_name=table_name,
                   deletion_columns=deletion_columns,
                   deletion_queue_bucket=deletion_queue_bucket,
                   deletion_queue_key=deletion_queue_key)


def camel_to_snake_case(value):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', value).lower()


def validate_mapper(mapper):
    # TODO:: consider dropping that
    if any(len(mapper["Columns"]) != n for n in get_existing_number_of_columns()):
        raise ValueError("All data mappers must have the same number of match keys")
    existing_s3_locations = get_existing_s3_locations()
    if mapper["QueryExecutorParameters"].get("DataCatalogProvider") == "glue":
        table_details = get_table_details_from_mapper(mapper)
        new_location = get_glue_table_location(table_details)
        format_info = get_glue_table_format(table_details)
        if any([is_overlap(new_location, e) for e in existing_s3_locations]):
            raise ValueError(
                "A data mapper already exists which covers this S3 location"
            )
        if format_info[2] not in SUPPORTED_SERDE_LIBS:
            raise ValueError(
                "The format for the specified table is not supported. The SerDe lib must be one of {}".format(
                    ", ".join(SUPPORTED_SERDE_LIBS)
                )
            )


def get_existing_number_of_columns():
    items = table.scan()["Items"]
    return [len(mapper["Columns"]) for mapper in items]


def get_existing_s3_locations():
    items = table.scan()["Items"]
    glue_mappers = [
        get_table_details_from_mapper(mapper)
        for mapper in items
        if mapper["QueryExecutorParameters"].get("DataCatalogProvider") == "glue"
    ]
    return [get_glue_table_location(m) for m in glue_mappers]


def get_table_details_from_mapper(mapper):
    db = mapper["QueryExecutorParameters"]["Database"]
    table_name = mapper["QueryExecutorParameters"]["Table"]
    return glue_client.get_table(DatabaseName=db, Name=table_name)


def get_glue_table_location(t):
    return t["Table"]["StorageDescriptor"]["Location"]


def get_glue_table_format(t):
    return (
        t["Table"]["StorageDescriptor"]["InputFormat"],
        t["Table"]["StorageDescriptor"]["OutputFormat"],
        t["Table"]["StorageDescriptor"]["SerdeInfo"]["SerializationLibrary"],
    )


def is_overlap(a, b):
    return a in b or b in a
