"""
DataMapper handlers
"""
import json
import os

import boto3

from boto_utils import get_user_info, running_job_exists
from decorators import with_logging, request_validator, catch_errors, add_cors_headers, json_body_loader, load_schema

dynamodb_resource = boto3.resource("dynamodb")
table = dynamodb_resource.Table(os.getenv("DataMapperTable"))
glue_client = boto3.client("glue")

SUPPORTED_SERDE_LIBS = [
    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
]


@with_logging
@add_cors_headers
@catch_errors
def get_data_mappers_handler(event, context):
    items = table.scan()["Items"]

    return {
        "statusCode": 200,
        "body": json.dumps({"DataMappers": items})
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
    item = {
        "DataMapperId": path_params["data_mapper_id"],
        "Columns": body["Columns"],
        "Format": body.get("Format", "parquet"),
        "QueryExecutor": body["QueryExecutor"],
        "QueryExecutorParameters": body["QueryExecutorParameters"],
        "CreatedBy": get_user_info(event),
        "RoleArn": body.get("RoleArn", None)
    }
    item = {k: v for k, v in item.items() if v is not None}
    table.put_item(Item=item)

    return {
        "statusCode": 201,
        "body": json.dumps(item)
    }


@with_logging
@add_cors_headers
@request_validator(load_schema("delete_data_mapper"))
@catch_errors
def delete_data_mapper_handler(event, context):
    if running_job_exists():
        raise ValueError("Cannot delete Data Mappers whilst there is a job in progress")
    data_mapper_id = event["pathParameters"]["data_mapper_id"]
    table.delete_item(Key={
        "DataMapperId": data_mapper_id
    })

    return {
        "statusCode": 204
    }


def validate_mapper(mapper):
    existing_s3_locations = get_existing_s3_locations()
    if mapper["QueryExecutorParameters"].get("DataCatalogProvider") == "glue":
        table_details = get_table_details_from_mapper(mapper)
        new_location = get_glue_table_location(table_details)
        format_info = get_glue_table_format(table_details)
        if any([is_overlap(new_location, e) for e in existing_s3_locations]):
            raise ValueError("A data mapper already exists which covers this S3 location")
        if format_info[2] not in SUPPORTED_SERDE_LIBS:
            raise ValueError("The format for the specified table is not supported. The SerDe lib must be one of {}"
                             .format(", ".join(SUPPORTED_SERDE_LIBS)))


def get_existing_s3_locations():
    items = table.scan()["Items"]
    glue_mappers = [
        get_table_details_from_mapper(mapper) for mapper in items
        if mapper["QueryExecutorParameters"].get("DataCatalogProvider") == "glue"
    ]
    return [
        get_glue_table_location(m) for m in glue_mappers
    ]


def get_table_details_from_mapper(mapper):
    db = mapper["QueryExecutorParameters"]["Database"]
    table_name = mapper["QueryExecutorParameters"]["Table"]
    return glue_client.get_table(DatabaseName=db, Name=table_name)


def get_glue_table_location(t):
    return t["Table"]["StorageDescriptor"]["Location"]


def get_glue_table_format(t):
    return t["Table"]["StorageDescriptor"]["InputFormat"], \
           t["Table"]["StorageDescriptor"]["OutputFormat"], \
           t["Table"]["StorageDescriptor"]["SerdeInfo"]["SerializationLibrary"]


def is_overlap(a, b):
    return a in b or b in a
