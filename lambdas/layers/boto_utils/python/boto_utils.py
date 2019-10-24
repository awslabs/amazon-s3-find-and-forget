def paginate(client, method, iter_key, **kwargs):
    paginator = client.get_paginator(method.__name__)
    page_iterator = paginator.paginate(**kwargs)
    for page in page_iterator:
        for result in page[iter_key]:
            yield result
