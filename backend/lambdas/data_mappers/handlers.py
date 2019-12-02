"""
DataMapper handlers
"""
import json
import os

import boto3
from aws_xray_sdk.core import xray_recorder

from decorators import with_logger, request_validator, catch_errors, load_schema

dynamodb_resource = boto3.resource("dynamodb")
table = dynamodb_resource.Table(os.getenv("DataMapperTable"))
glue_client = boto3.client("glue")


@with_logger
@xray_recorder.capture('GetDataMappersHandler')
@catch_errors
def get_data_mappers_handler(event, context):
    items = table.scan()["Items"]

    return {
        "statusCode": 200,
        "body": json.dumps({"DataMappers": items})
    }


@with_logger
@xray_recorder.capture('CreateDataMapperHandler')
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
@request_validator(load_schema("delete_handler"), "pathParameters")
@catch_errors
def delete_data_mapper_handler(event, context):
    data_mapper_id = event["pathParameters"]["data_mapper_id"]
    table.delete_item(Key={
        "DataMapperId": data_mapper_id
    })

    return {
        "statusCode": 204,
    }


def validate_mapper(mapper):
    existing_s3_locations = get_existing_s3_locations()
    if mapper["QueryExecutorParameters"].get("DataCatalogProvider") == "glue":
        table_details = get_table_details_from_mapper(mapper)
        new_location = get_glue_table_location(
            table_details[0],
            table_details[1]
        )
        if any([is_overlap(new_location, e) for e in existing_s3_locations]):
            raise ValueError("A data mapper already exists which covers this S3 location")


def get_existing_s3_locations():
    items = table.scan()["Items"]
    glue_mappers = [
        get_table_details_from_mapper(mapper) for mapper in items
        if mapper["QueryExecutorParameters"].get("DataCatalogProvider") == "glue"
    ]
    return [
        get_glue_table_location(m[0], m[1]) for m in glue_mappers
    ]


def get_table_details_from_mapper(mapper):
    return (
        mapper["QueryExecutorParameters"]["Database"],
        mapper["QueryExecutorParameters"]["Table"]
    )


def get_glue_table_location(db, table_name):
    t = glue_client.get_table(DatabaseName=db, Name=table_name)
    return t["Table"]["StorageDescriptor"]["Location"]


def is_overlap(a, b):
    return a in b or b in a
