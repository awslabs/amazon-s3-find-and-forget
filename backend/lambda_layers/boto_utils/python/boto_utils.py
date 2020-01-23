import calendar
from datetime import datetime
import decimal
import json
import os
import time
import uuid

batch_size = 10  # SQS Max Batch Size


def paginate(client, method, iter_keys, **kwargs):
    paginator = client.get_paginator(method.__name__)
    page_iterator = paginator.paginate(**kwargs)
    if isinstance(iter_keys, str):
        iter_keys = [iter_keys]
    for page in page_iterator:
        for key in iter_keys:
            page = page[key]
        for result in page:
            yield result


def read_queue(queue, number_to_read=10):
    msgs = []
    while len(msgs) < number_to_read:
        received = queue.receive_messages(
            MaxNumberOfMessages=min((number_to_read - len(msgs)), batch_size),
            AttributeNames=['All']
        )
        if len(received) == 0:
            break  # no messages left
        remaining = number_to_read - len(msgs)
        i = min(remaining, len(received))  # take as many as allowed from the received messages
        msgs = msgs + received[:i]
    return msgs


def batch_sqs_msgs(queue, messages, **kwargs):
    chunks = [messages[x:x + batch_size] for x in range(0, len(messages), batch_size)]
    for chunk in chunks:
        entries = [
            {
                'Id': str(uuid.uuid4()),
                'MessageBody': json.dumps(m),
                **({'MessageGroupId': str(uuid.uuid4())} if queue.attributes.get("FifoQueue", False) else {}),
                **kwargs,
            } for m in chunk
        ]
        queue.send_messages(Entries=entries)


def log_event(client, log_stream, event_name, event_data):
    log_group = os.getenv("LogGroupName", "/aws/s3f2/")
    kwargs = {
        "logGroupName": log_group,
        "logStreamName": log_stream,
        "logEvents": [
            {
                'timestamp': int(round(time.time() * 1000)),
                'message': json.dumps({
                    "EventName": event_name,
                    "EventData": event_data
                })
            },
        ],
    }
    sequence_token = create_stream_if_not_exists(client, log_group, log_stream)
    if sequence_token:
        kwargs["sequenceToken"] = sequence_token

    client.put_log_events(**kwargs)


def create_stream_if_not_exists(client, log_group, log_stream):
    """
    Creates a log stream if it doesn't already exist
    otherwise returns the sequence token needed to
    write to the stream
    """
    response = client.describe_log_streams(
        logGroupName=log_group,
        logStreamNamePrefix=log_stream,
    )["logStreams"]
    if len(response) == 0:
        client.create_log_stream(
            logGroupName=log_group,
            logStreamName=log_stream
        )
    else:
        return response[0].get("uploadSequenceToken")


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return round(o)
        return super(DecimalEncoder, self).default(o)


def convert_iso8601_to_epoch(iso_time: str):
    parsed = datetime.strptime(iso_time, "%Y-%m-%dT%H:%M:%S.%fZ")
    unix_timestamp = calendar.timegm(parsed.timetuple())
    return unix_timestamp
