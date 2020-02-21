import functools
import inspect
import json
import logging
import os
from uuid import uuid4

import boto3
import jsonschema
from botocore.exceptions import ClientError

from boto_utils import DecimalEncoder, parse_s3_url

logger = logging.getLogger()
logger.setLevel(os.getenv("LogLevel", logging.INFO))

s3 = boto3.resource("s3")


def with_logger(handler):
    """
    Decorator which performs basic logging and makes logger available on context
    """

    @functools.wraps(handler)
    def wrapper(event, context):
        logger.info("## HANDLER: %s", handler.__name__)
        logger.debug("## ENVIRONMENT VARIABLES")
        logger.debug(json.dumps(os.environ.copy()))
        logger.info("## EVENT")
        logger.info(json.dumps(event))
        context.logger = logger
        return handler(event, context)

    return wrapper


def json_body_loader(handler):
    """
    Decorator which loads the JSON body of a request
    """
    @functools.wraps(handler)
    def wrapper(event, context):
        if isinstance(event.get("body"), str):
            event["body"] = json.loads(event["body"])

        return handler(event, context)

    return wrapper


def request_validator(request_schema):
    """
    Decorator which performs JSON validation on an event
    """

    def wrapper_wrapper(handler):
        @functools.wraps(handler)
        def wrapper(to_validate, *args, **kwargs):
            try:
                jsonschema.validate(to_validate, request_schema)
            except (KeyError, jsonschema.exceptions.SchemaError) as e:
                logger.fatal("Invalid configuration: {}".format(str(e)))
                return {
                    "statusCode": 500,
                    "body": json.dumps({
                        "Message": "Invalid configuration: ".format(str(e)),
                    })
                }
            except jsonschema.ValidationError as exception:
                logger.error("Invalid Request: {}".format(exception.message))
                return {
                    "statusCode": 422,
                    "body": json.dumps({
                        "Message": "Invalid Request: {}".format(exception.message),
                    })
                }

            return handler(to_validate, *args, **kwargs)

        return wrapper

    return wrapper_wrapper


def catch_errors(handler):
    """
    Decorator which performs catch all exception handling
    """

    @functools.wraps(handler)
    def wrapper(event, context):
        try:
            return handler(event, context)
        except ClientError as e:
            logger.error("boto3 client error: {}".format(str(e)))
            return {
                "statusCode": e.response['ResponseMetadata'].get('HTTPStatusCode', 400),
                "body": json.dumps({
                    "Message": "Client error: {}".format(str(e)),
                })
            }
        except ValueError as e:
            logger.warning("Invalid request: {}".format(str(e)))
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "Message": "Invalid request: {}".format(str(e)),
                })
            }
        except Exception as e:
            # Unknown error so avoid leaking any info
            logger.error("Error handling event: {}".format(str(e)))
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "Message": "Unable to process request: {}".format(str(e)),
                })
            }

    return wrapper


def load_schema(schema_name, schema_dir=None):
    if not schema_dir:
        caller_dir = os.path.dirname(os.path.abspath((inspect.stack()[1])[1]))
        schema_dir = os.path.join(caller_dir, "schemas")

    with open(os.path.join(schema_dir, "{}.json".format(schema_name))) as f:
        return json.load(f)


def add_cors_headers(handler):
    """
    Decorator which returns standard response headers to be used on each API method
    """

    @functools.wraps(handler) 
    def wrapper(event, context): 
        resp = handler(event, context) 
        resp["headers"] = { 
           'Content-Type': 'application/json', 
           'Access-Control-Allow-Origin': os.getenv("AllowOrigin", ""), 
           **resp.get("headers", {}) 
        }

        return resp
 
    return wrapper


def s3_state_store(load_keys=[], offload_keys=[], should_offload=True, should_load=True,
                   bucket=None, prefix="state/"):
    """
    Decorator which auto (re)stores state to/from S3.
    Only dictionaries and lists can be (re)stored to/from S3
    """
    if not bucket:
        bucket = os.getenv("StateBucket")

    def _load_value(value):
        parsed_bucket, parsed_key = parse_s3_url(value)
        logger.info("Loading data from S3 key {}".format(parsed_key))
        obj = s3.Object(parsed_bucket, parsed_key).get()["Body"].read()
        return json.loads(obj)

    def _offload_value(value):
        key = "{}{}".format(prefix, uuid4())
        logger.info("Offloading data to S3 key {}".format(key))
        s3.Object(bucket, key).put(Body=json.dumps(value, cls=DecimalEncoder))
        return "s3://{}/{}".format(bucket, key)

    def load(d):
        loaded = {}

        for k, v in d.items():
            if (k in load_keys or len(load_keys) == 0) and isinstance(v, str) and v.startswith("s3://"):
                loaded[k] = _load_value(v)
            elif isinstance(v, dict):
                loaded[k] = load(v)
            else:
                loaded[k] = v
        return loaded

    def offload(d):
        offloaded = {}

        for k, v in d.items():
            if (k in offload_keys or len(offload_keys) == 0) and isinstance(v, (dict, list)):
                offloaded[k] = _offload_value(v)
            elif isinstance(v, dict):
                offloaded[k] = offload(v)
            else:
                offloaded[k] = v

        return offloaded

    def wrapper_wrapper(handler):
        @functools.wraps(handler)
        def wrapper(event, context):
            if should_load and isinstance(event, dict):
                event = load(event)

            resp = handler(event, context)

            if should_offload and isinstance(resp, dict):
                resp = offload(resp)
            return resp
        return wrapper
    return wrapper_wrapper
