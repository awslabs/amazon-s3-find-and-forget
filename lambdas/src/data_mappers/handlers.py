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
    item = {
        "DataMapperId": path_params["data_mapper_id"],
        "Columns": body["Columns"],
        "Format": body.get("Format", "parquet"),
        "DataSource": body["DataSource"],
        "DataSourceParameters": body["DataSourceParameters"]
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

