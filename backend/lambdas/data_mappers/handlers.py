"""
DataMapper handlers
"""
import json
import os

import boto3
from aws_xray_sdk.core import xray_recorder

from decorators import with_logger, request_validator, catch_errors, load_schema, add_cors_headers

dynamodb_resource = boto3.resource("dynamodb")
table = dynamodb_resource.Table(os.getenv("DataMapperTable"))
glue_client = boto3.client("glue")

SUPPORTED_SERDE_LIBS = [
    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
]


@with_logger
@xray_recorder.capture('GetDataMappersHandler')
@add_cors_headers
@catch_errors
def get_data_mappers_handler(event, context):
    items = table.scan()["Items"]

    return {
        "statusCode": 200,
        "body": json.dumps({"DataMappers": items})
    }


@with_logger
@xray_recorder.capture('CreateDataMapperHandler')
@add_cors_headers
@request_validator(load_schema("data_mapper"))
@request_validator(load_schema("data_mapper_path_parameters"), "pathParameters")
@catch_errors
def create_data_mapper_handler(event, context):
    path_params = event["pathParameters"]
    body = json.loads(event["body"])
    validate_mapper(body)
    item = {
        "DataMapperId": path_params["data_mapper_id"],
        "Columns": body["Columns"],
        "Format": body.get("Format", "parquet"),
        "QueryExecutor": body["QueryExecutor"],
        "QueryExecutorParameters": body["QueryExecutorParameters"]
    }
    table.put_item(Item=item)

    return {
        "statusCode": 201,
        "body": json.dumps(item)
    }


@with_logger
@xray_recorder.capture('DeleteDataMapperHandler')
@add_cors_headers
@request_validator(load_schema("delete_handler"), "pathParameters")
@catch_errors
def delete_data_mapper_handler(event, context):
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
