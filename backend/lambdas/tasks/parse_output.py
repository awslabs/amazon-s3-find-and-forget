import json

from decorators import with_logger


@with_logger
def handler(event, context):
    return json.loads(event)

