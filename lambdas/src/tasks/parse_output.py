import json


def handler(event, context):
    return json.loads(event)

