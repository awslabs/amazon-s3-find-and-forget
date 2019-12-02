import functools
import inspect
import json
import logging
import os

import jsonschema
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.getenv("LogLevel", logging.INFO))


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


def request_validator(request_schema, payload_key="body"):
    """
    Decorator which performs JSON validation on an event key
    """

    def wrapper_wrapper(handler):
        @functools.wraps(handler)
        def wrapper(event, context):
            try:
                to_validate = event[payload_key]
                if isinstance(to_validate, str):
                    to_validate = json.loads(to_validate)
                jsonschema.validate(to_validate, request_schema)
            except KeyError as e:
                logger.fatal("Invalid payload key: {}".format(str(e)))
                return {
                    "statusCode": 500
                }
            except jsonschema.ValidationError as exception:
                logger.error("Invalid Request: {}".format(exception.message))
                return {
                    "statusCode": 422,
                    "body": exception.message,
                }

            return handler(event, context)

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
                "statusCode": e.response['ResponseMetadata'].get('HTTPStatusCode', 400)
            }
        except ValueError as e:
            logger.warning("Invalid request: {}".format(str(e)))
            return {
                "statusCode": 400,
                "body": str(e),
            }
        except Exception as e:
            # Unknown error so avoid leaking any info
            logger.error("Error handling event: {}".format(str(e)))
            return {
                "statusCode": 400,
                "body": "Unable to process request",
            }

    return wrapper


def load_schema(schema_name, schema_dir=None):
    if not schema_dir:
        caller_dir = os.path.dirname(os.path.abspath((inspect.stack()[1])[1]))
        schema_dir = os.path.join(caller_dir, "schemas")

    with open(os.path.join(schema_dir, "{}.json".format(schema_name))) as f:
        return json.load(f)
