"""
Settings handlers
"""
import json

import boto3

from boto_utils import get_config, DecimalEncoder
from decorators import with_logging, catch_errors, add_cors_headers


@with_logging
@add_cors_headers
@catch_errors
def list_settings_handler(event, context):
    config = get_config()
    return {
        "statusCode": 200,
        "body": json.dumps({"Settings": config}, cls=DecimalEncoder),
    }
