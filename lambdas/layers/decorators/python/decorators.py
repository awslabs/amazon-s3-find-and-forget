import json
import logging
import os
import functools

logger = logging.getLogger()
logger.setLevel(os.getenv("LogLevel", logging.INFO))


def with_logger(func):
    """
    Decorator which performs basic logging and makes logger available on context
    """
    @functools.wraps(func)
    def wrapper(event, context):
        logger.info("## HANDLER: %s", func.__name__)
        logger.debug("## ENVIRONMENT VARIABLES")
        logger.debug(json.dumps(os.environ.copy()))
        logger.info("## EVENT")
        logger.info(json.dumps(event))
        context.logger = logger
        return func(event, context)

    return wrapper
