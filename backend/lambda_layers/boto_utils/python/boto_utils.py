import json
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


def batch_sqs_msgs(queue, queries):
    chunks = [queries[x:x + batch_size] for x in range(0, len(queries), batch_size)]
    for chunk in chunks:
        entries = [
            {
                'Id': str(uuid.uuid4()),
                'MessageBody': json.dumps(q),
            } for q in chunk
        ]
        queue.send_messages(Entries=entries)
