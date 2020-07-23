import json

from decorators import with_logging


@with_logging
def handler(event, context):
    return json.loads(event)
