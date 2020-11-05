import json

from decorators import with_logging


@with_logging
def handler(event, context):
    """
    Handle an event handler.

    Args:
        event: (todo): write your description
        context: (dict): write your description
    """
    return json.loads(event)
