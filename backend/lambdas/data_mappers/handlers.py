"""
DataMapper handlers
"""
import json
import os
import re
import time
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

s3f2_flow_bucket = os.getenv("FlowBucket", "")

dynamodb_resource = boto3.resource("dynamodb")
table = dynamodb_resource.Table(os.getenv("DataMapperTable"))

glue_client = boto3.client("glue")
athena_client = boto3.client("athena")

PARQUET_HIVE_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
JSON_HIVE_SERDE = "org.apache.hive.hcatalog.data.JsonSerDe"
JSON_OPENX_SERDE = "org.openx.data.jsonserde.JsonSerDe"
SUPPORTED_SERDE_LIBS = [PARQUET_HIVE_SERDE, JSON_HIVE_SERDE, JSON_OPENX_SERDE]


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
    data_mapper = table.get_item(Key={"DataMapperId": data_mapper_id})["Item"]
    query = "DROP TABLE {}.{}".format(data_mapper["DeletionQueueDb"], data_mapper["DeletionQueueTableName"])
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            "OutputLocation": "s3://{bucket}/{prefix}/".format(
                bucket=s3f2_flow_bucket, prefix="data_mappers/queries/drop/"
            )
        },
        WorkGroup=os.getenv("WorkGroup", "primary"),
    )
    if is_athena_query_successful(response):
        table.delete_item(Key={"DataMapperId": data_mapper_id})
    else:
        raise ValueError("Failed to delete Deletion Queue Athena Table for Data Mapper")
    return {"statusCode": 204}


def is_athena_query_successful(response):
    res = False
    for i in range(10):
        new_response = athena_client.get_query_execution(
            QueryExecutionId=response['QueryExecutionId']
        )
        if new_response['QueryExecution']['Status']['State'] not in ['RUNNING', 'QUEUED']:
            break
        time.sleep(3)

    if new_response['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        res = True
    return res


def generate_athena_table_for_mapper(mapper, deletion_db, deletion_table_name, deletion_queue_bucket, deletion_queue_key):
    response = athena_client.start_query_execution(
        QueryString=make_query(mapper, deletion_db, deletion_table_name, deletion_queue_bucket, deletion_queue_key),
        ResultConfiguration={
            "OutputLocation": "s3://{bucket}/{prefix}/".format(
                bucket=s3f2_flow_bucket, prefix="data_mappers/queries/create"
            )
        },
        WorkGroup=os.getenv("WorkGroup", "primary"),
    )
    if not is_athena_query_successful(response):
        raise ValueError("Failed to Create Deletion Queue Athena Table for Data Mapper")


def make_query(mapper, db, table_name, deletion_queue_bucket, deletion_queue_key):
    t = get_table_details_from_mapper(mapper)["Table"]
    columns = t["StorageDescriptor"]["Columns"]
    x = ["`{col_name}` {col_type}".format(col_name=c["Name"], col_type=c["Type"])
         for c in columns if c["Name"] in mapper["Columns"]]
    deletion_columns = ", ".join(x)
    query = """
            CREATE EXTERNAL TABLE {db}.{table_name}(
              {deletion_columns}
              )
            ROW FORMAT SERDE 
              'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
            WITH SERDEPROPERTIES ( 
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
    print(query)
    return query


def camel_to_snake_case(value):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', value).lower()


def validate_mapper(mapper):
    existing_s3_locations = get_existing_s3_locations()
    if mapper["QueryExecutorParameters"].get("DataCatalogProvider") == "glue":
        table_details = get_table_details_from_mapper(mapper)
        new_location = get_glue_table_location(table_details)
        serde_lib, serde_params = get_glue_table_format(table_details)
        if any([is_overlap(new_location, e) for e in existing_s3_locations]):
            raise ValueError(
                "A data mapper already exists which covers this S3 location"
            )
        if serde_lib not in SUPPORTED_SERDE_LIBS:
            raise ValueError(
                "The format for the specified table is not supported. The SerDe lib must be one of {}".format(
                    ", ".join(SUPPORTED_SERDE_LIBS)
                )
            )
        if serde_lib == JSON_OPENX_SERDE:
            not_allowed_json_params = {
                "ignore.malformed.json": "TRUE",
                "dots.in.keys": "TRUE",
            }
            for param, value in not_allowed_json_params.items():
                if param in serde_params and serde_params[param] == value:
                    raise ValueError(
                        "The parameter {} cannot be {} for SerDe library {}".format(
                            param, value, JSON_OPENX_SERDE
                        )
                    )
            if any([k for k, v in serde_params.items() if k.startswith("mapping.")]):
                raise ValueError(
                    "Column mappings are not supported for SerDe library {}".format(
                        JSON_OPENX_SERDE
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
        t["Table"]["StorageDescriptor"]["SerdeInfo"]["SerializationLibrary"],
        t["Table"]["StorageDescriptor"]["SerdeInfo"]["Parameters"],
    )


def is_overlap(a, b):
    return a in b or b in a
